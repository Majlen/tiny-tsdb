mod db;
mod parser;
use anyhow::Result;
use clap::Parser;
use db::datapoint::Datapoint;
use parser::SqlStatement;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::collections::HashMap;
use std::ops::{Add, Sub};
use std::str;
use std::time::{Duration, SystemTime};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long)]
    database_dir: String,
}

fn testing_stuff(db: &impl db::DB) -> Result<()> {
    let time = SystemTime::now();
    let dp = db::datapoint::Datapoint {
        metric: "test".to_owned(),
        value: 1.5,
        time: time,
        tags: HashMap::new(),
    };
    println!("{}", dp.to_key_string());
    let tags = HashMap::from([
        ("test".to_owned(), "bla".to_owned()),
        ("xxx".to_owned(), "yyy".to_owned()),
    ]);
    let dp2 = db::datapoint::Datapoint {
        metric: "metric".to_owned(),
        value: 2.3,
        time: time,
        tags: tags,
    };
    println!("{}", dp2.to_key_string());

    println!("{:?}", db.get(db::MAX_METRIC_ID_KEY)?);
    db.put_datapoint(dp);
    println!("{:?}", db.get(db::MAX_METRIC_ID_KEY)?);
    db.put_datapoint(dp2);
    println!("{:?}", db.get(db::MAX_METRIC_ID_KEY)?);
    println!("{:?}", db.get("test#")?);
    println!("{:?}", db.get("metric#test:bla,xxx:yyy")?);
    println!("{:?}", db.get("metric#xxx:yyy,test:bla")?);

    let bucket = (SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_secs() / 60) * 60;
    println!("{:?}", db.get(format!("{}##{}", bucket, 1).as_ref())?);
    println!("{:?}", db.get(format!("{}##{}", bucket, 2).as_ref())?);

    println!("{:?}", db.get_datapoints_in_bucket("test", &HashMap::new(), &time));
    println!("{:?}", db.get_datapoints_exact("test", &HashMap::new(), &time.sub(Duration::from_secs(3600)), &time.add(Duration::from_secs(60)))?);
    Ok(())
}

fn run_cmd(sql: SqlStatement, db: &impl db::DB) -> Result<()> {
    use parser::select::Operator;
    match sql {
        SqlStatement::Select(s) => {
            let mut start_time = SystemTime::UNIX_EPOCH;
            let mut end_time = SystemTime::UNIX_EPOCH.add(Duration::from_secs(u32::MAX.into()));
            let mut tags = HashMap::<String, String>::new();
            for c in s.conditions {
                if c.field == "time" {
                    let time = SystemTime::UNIX_EPOCH.add(Duration::from_secs(c.value.parse()?));
                    match c.operator {
                        Operator::Ge => start_time = time,
                        Operator::Gt => start_time = time.add(Duration::from_secs(1)),
                        Operator::Le => end_time = time,
                        Operator::Lt => end_time = time.sub(Duration::from_secs(1)),
                        Operator::Eq => {
                            start_time = time;
                            end_time = time;
                        },
                        Operator::Ne => {
                            // Invalid, deal with it later
                            end_time = SystemTime::UNIX_EPOCH;
                        }
                    }
                    continue;
                }
                // Again, more operators not supported yet
                if c.operator == Operator::Eq {
                    tags.insert(c.field, c.value);
                }
            }
            let mut results = Vec::new();
            for field in s.fields {
                let batch = db.get_datapoints_exact(&field, &tags, &start_time, &end_time)?;
                results.extend(batch);
            }
            println!("{:?}", results);
            Ok(())
        },
        SqlStatement::Insert(i) => {
            let mut dp = Datapoint::default();
            for (key, value) in i.values {
                match key.as_ref() {
                    "time" => {
                        dp.time = SystemTime::UNIX_EPOCH.add(Duration::from_secs(value.parse::<u64>()?));
                    },
                    s => {
                        match value.parse::<f64>() {
                            Ok(v) => {
                                dp.value = v;
                                dp.metric = s.to_owned();
                            },
                            Err(_) => {
                                dp.tags.insert(s.to_owned(), value);
                            }
                        }
                    }
                }
            }
            return db.put_datapoint(dp);
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let db = db::rocksdb::RocksDB::new(&args.database_dir)?;

    let mut editor = Editor::<()>::new();
    loop {
        let line = editor.readline("SQL > ");
        match line {
            Ok(cmd) => {
                editor.add_history_entry(&cmd);
                let cmd = parser::parse(&cmd);
                match cmd {
                    Ok((_, sql)) => run_cmd(sql, &db),
                    Err(e) => {
                        println!("{:?}", e);
                        Ok(())
                    }
                };
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    Ok(())
}
