use sqlparser::{dialect::GenericDialect, parser::Parser};
use ssdb::error::Result;

fn main() -> Result<()> {
    // 循环读取 STDIN 并解析为 SQL 语句
    loop {
        let mut buf = String::new();
        std::io::stdin().read_line(&mut buf)?;
        match Parser::new(&GenericDialect {}).try_with_sql(&buf) {
            Ok(mut p) => {
                let ast = p.parse_statement();
                if ast.is_err() {
                    println!("{:#?}", ast);
                    continue;
                }
                println!("{:#?}", ast.unwrap());
            }
            Err(e) => {
                println!("{:#?}", e);
                continue;
            }
        }
    }
}
