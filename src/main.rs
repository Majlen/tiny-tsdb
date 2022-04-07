mod parser;

use rustyline::Editor;
use rustyline::error::ReadlineError;

fn main() {
    let mut editor = Editor::<()>::new();
    loop {
        let line = editor.readline("SQL > ");
        match line {
            Ok(cmd) => {
                editor.add_history_entry(&cmd);
                let cmd = parser::parse(&cmd);
                match cmd {
                    Ok(sql) => println!("{:?}", sql),
                    Err(e) => println!("{:?}", e),
                }
            },
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break
            },
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break
            },
            Err(err) => {
                println!("Error: {:?}", err);
                break
            }
        }
    }
}