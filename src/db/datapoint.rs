use std::collections::HashMap;
use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct Datapoint {
    pub metric: String,
    pub value: f64,
    pub time: SystemTime,
    pub tags: HashMap<String, String>,
}

impl Datapoint {
    pub fn default() -> Self {
        Datapoint {
            metric: String::default(),
            value: 0.0,
            time: SystemTime::UNIX_EPOCH,
            tags: HashMap::default(),
        }
    }

    pub fn to_key_string(&self) -> String {
        Datapoint::key_string(self.metric.as_ref(), &self.tags)
    }

    pub fn key_string(metric: &str, tags: &HashMap<String, String>) -> String {
        let mut output = String::new();
        output.push_str(metric);
        output.push('#');
        let mut first = true;
        for (key, value) in tags {
            if first {
                first = false;
            } else {
                output.push_str(",");
            }
            output.push_str(&format!("{}:{}", key, value));
        }
        output
    }
}
