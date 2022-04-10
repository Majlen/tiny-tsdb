use anyhow::Result;
use rocksdb::{DBCompressionType, Options, DB};
use std::cmp;
use std::collections::HashMap;
use std::convert::TryInto;
use std::ops::Add;
use std::str;
use std::time::{Duration, SystemTime};

pub mod datapoint;

pub const MAX_METRIC_ID_KEY: &str = "###INTERNAL_MAX_METRIC";
const SECS_IN_MINUTE: u64 = 60;

pub struct Database {
    db: DB,
}

impl Database {
    pub fn new(path: &str) -> Result<Database> {
        let mut options = Options::default();
        options.set_compression_type(DBCompressionType::Zstd);
        options.create_if_missing(true);
        let db = DB::open(&options, path)?;
        Ok(Database { db: db })
    }

    pub fn put(&self, key: &str, val: &[u8]) -> Result<()> {
        self.db.put(key, val)?;
        Ok(())
    }

    fn get_id(&self, key: &str) -> Result<Option<u64>> {
        let value = self.db.get(key)?;
        match value {
            Some(vector) => return Ok(Some(u64::from_le_bytes(vector[0..8].try_into()?))),
            None => return Ok(None),
        }
    }

    fn get_max_metric_id(&self) -> Result<u64> {
        let value = self.get_id(MAX_METRIC_ID_KEY)?;
        match value {
            Some(id) => return Ok(id),
            None => Ok(0),
        }
    }

    fn get_id_or_register(&self, key: &str) -> Result<u64> {
        let value = self.get_id(key)?;
        match value {
            Some(id) => Ok(id),
            None => {
                let mut id = self.get_max_metric_id()?;
                id += 1;
                self.put(key, &id.to_le_bytes())?;
                self.put(MAX_METRIC_ID_KEY, &id.to_le_bytes())?;
                return Ok(id);
            }
        }
    }

    fn select_time_bucket_and_offset(&self, time: SystemTime) -> Result<(u64, u64)> {
        let secs = time.duration_since(SystemTime::UNIX_EPOCH)?.as_secs();
        let time_bucket = (secs / SECS_IN_MINUTE) * SECS_IN_MINUTE;
        let offset = secs % SECS_IN_MINUTE;
        Ok((time_bucket, offset))
    }

    fn format_data(data: Vec<u8>, value: f64, offset: u64) -> Vec<u8> {
        let mut outdata: Vec<u8> = vec![0; cmp::max(data.len() + 8, 16)];
        if data.len() >= 8 {
            outdata[0..8].copy_from_slice(&data[0..8]);
        }
        let mut position = 0;
        let mut occupied = false;
        let mut total_elements = 0;
        let mut bitmap = u64::from_le_bytes(outdata[0..8].try_into().unwrap());

        for i in 0..SECS_IN_MINUTE {
            let mask = 1u64 << i;
            let populated = bitmap & mask != 0;
            if i == offset {
                position = total_elements;
                if populated {
                    occupied = true;
                }
            }
            if populated {
                total_elements += 1;
            }
        }

        // Set the field in bitmap
        bitmap |= 1u64 << offset;
        outdata[0..8].copy_from_slice(&bitmap.to_le_bytes());
        let value_position = (8 + position * 8) as usize;
        let end = (8 + total_elements * 8) as usize;

        if position == total_elements || occupied {
            if position != 0 {
                outdata[8..end].copy_from_slice(&data[8..end]);
            }
            outdata[value_position..(value_position + 8)].copy_from_slice(&value.to_le_bytes());
        } else {
            outdata[8..value_position].copy_from_slice(&data[8..value_position]);
            outdata[value_position..(value_position + 8)].copy_from_slice(&value.to_le_bytes());
            outdata[(value_position + 8)..(end + 8)].copy_from_slice(&data[value_position..end]);
        }
        outdata
    }

    fn parse_data(
        input: Option<Vec<u8>>,
        metric: &str,
        tags: &HashMap<String, String>,
        time_bucket: SystemTime,
    ) -> Vec<datapoint::Datapoint> {
        let data = input.unwrap_or(vec![0; 8]);
        let bitmap = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let mut positions = Vec::new();

        for i in 0..SECS_IN_MINUTE {
            let mask = 1u64 << i;
            let populated = bitmap & mask != 0;
            if populated {
                positions.push(i);
            }
        }

        let mut output = Vec::new();
        for i in 0..positions.len() {
            let start = 8 + i * 8;
            let end = start + 8;
            let value = f64::from_le_bytes(data[start..end].try_into().unwrap());
            let time = time_bucket.add(Duration::new(positions[i], 0));
            let dp = datapoint::Datapoint{metric: metric.to_owned(), tags: tags.clone(), value: value, time: time};
            output.push(dp);
        }

        output
    }

    pub fn put_datapoint(&self, datapoint: datapoint::Datapoint) -> Result<()> {
        let metakey = datapoint.to_key_string();
        let id = self.get_id_or_register(metakey.as_ref())?;

        let (time_bucket, offset) = self.select_time_bucket_and_offset(datapoint.time)?;
        let datakey = format!("{}##{}", time_bucket, id);
        let mut datavalue = self.get(datakey.as_ref())?;
        datavalue = Database::format_data(datavalue, datapoint.value, offset);
        self.put(datakey.as_ref(), &datavalue)?;
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Vec<u8>> {
        let option = self.db.get(key)?;
        match option {
            Some(vector) => return Ok(vector),
            None => return Ok(vec![]),
        }
    }

    pub fn get_datapoints_in_bucket(
        &self,
        metric: &str,
        tags: &HashMap<String, String>,
        time_bucket: &SystemTime,
    ) -> Result<Vec<datapoint::Datapoint>> {
        // Make sure we have the time bucket aligned
        let mut time = time_bucket
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs();
        time = (time / SECS_IN_MINUTE) * SECS_IN_MINUTE;
        let key = datapoint::Datapoint::key_string(metric, tags);
        let id_opt = self.get_id(key.as_ref())?;

        match id_opt {
            Some(id) => {
                let points = self.db.get(format!("{}##{}", time, id))?;
                let system_time_bucket = SystemTime::UNIX_EPOCH.add(Duration::new(time, 0));
                return Ok(Database::parse_data(points, metric, tags, system_time_bucket));
            },
            None => return Ok(vec![]),
        }
    }

    pub fn get_datapoints_exact(
        &self,
        metric: &str,
        tags: &HashMap<String, String>,
        time_start: &SystemTime,
        time_end: &SystemTime,
    ) -> Result<Vec<datapoint::Datapoint>> {
        let mut current_time = time_start.clone();
        let mut results = Vec::<datapoint::Datapoint>::new();

        while current_time < *time_end {
            let batch = self.get_datapoints_in_bucket(metric, tags, &current_time)?;
            let filtered = batch.iter().filter(|e| e.time >= *time_start && e.time <= *time_end).cloned();
            results.extend(filtered);
            current_time += Duration::from_secs(SECS_IN_MINUTE);
        }

        Ok(results)
    }
}
