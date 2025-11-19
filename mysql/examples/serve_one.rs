// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! After running this, you should be able to run:
//!
//! ```console
//! $ echo "SELECT * FROM foo" | mysql -h 127.0.0.1 --table
//! ```

use std::io;
use tokio::io::AsyncWrite;

use opensrv_mysql::*;
use tokio::net::TcpListener;
use opensrv_mysql::Column;
use opensrv_mysql::ColumnType;
use opensrv_mysql::ColumnFlags;
// use opensrv_mysql::CharacterSet;
// use opensrv_mysql::OkResponse;
// use opensrv_mysql::AsyncMysqlShim;
use opensrv_mysql::AsyncMysqlIntermediary;
// use opensrv_mysql::StatementMetaWriter;
// use opensrv_mysql::QueryResultWriter;
// use opensrv_mysql::InitWriter;

struct Backend;

#[async_trait::async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for Backend {
    type Error = io::Error;


     /// Called when client switches database.
    async fn on_init<'a>(
        &'a mut self,
        database: &'a str,
        _: InitWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        print!("Init db, database:{}\n", database);
        Ok(())
    }

    async fn on_prepare<'a>(
        &'a mut self,
        pre_sql: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        println!("prepare sql {:?}", pre_sql);

        let param = Column {
            table: "foo".to_string(),
            column: "a".to_string(),
            collen: 2,
            coltype: ColumnType::MYSQL_TYPE_TINY,
            colflags: ColumnFlags::empty(),
        };

        let cols = [
            Column {
                table: "foo".to_string(),
                column: "a".to_string(),
                collen: 4,
                coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "foo".to_string(),
                column: "b".to_string(),
                collen: 4,
                coltype: ColumnType::MYSQL_TYPE_STRING,
                colflags: ColumnFlags::empty(),
            },
        ];
        info.reply(42, &[param.clone(),param], &cols).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        session_id: u32,
        param_parser: opensrv_mysql::ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        println!("execute with session_id:{} ", session_id);
        for (i, param) in param_parser.into_iter().enumerate() {
            // ParamValue no longer exposes Value/Null variants directly; print the param for now.
           // println!("  param[{}] = {:?}", i, param.value());
        }

        // let cols = [
        //     Column {
        //         table: "foo".to_string(),
        //         column: "a".to_string(),
        //         collen: 4,
        //         coltype: ColumnType::MYSQL_TYPE_LONGLONG,
        //         colflags: ColumnFlags::empty(),
        //     },
        //     Column {
        //         table: "foo".to_string(),
        //         column: "b".to_string(),
        //         collen: 4,
        //         coltype: ColumnType::MYSQL_TYPE_STRING,
        //         colflags: ColumnFlags::empty(),
        //     },
        // ];
        // let mut rw = results.start(&cols).await?;
        //
        // rw.write_col(55)?;
        // rw.write_col("execute result")?;
        //
        //
        // rw.finish().await;
        
        results.completed(OkResponse::default()).await
    }


    async fn on_close(&mut self, _: u32) {}

    async fn on_query<'a>(
        &'a mut self,
        sql: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        println!("query sql {:?}", sql);
        let cols = [
            // Column {
            //     table: "foo".to_string(),
            //     column: "a".to_string(),
            //     collen: 4,
            //     coltype: ColumnType::MYSQL_TYPE_LONGLONG,
            //     colflags: ColumnFlags::empty(),
            // },
            // Column {
            //     table: "foo".to_string(),
            //     column: "b".to_string(),
            //     collen: 8,
            //     coltype: ColumnType::MYSQL_TYPE_STRING,
            //     colflags: ColumnFlags::empty(),
            // },
        ];
        let mut rw = results.start(&cols).await?;

        // rw.write_col(42)?;
        // rw.write_col("b's value")?;


        rw.finish().await
    }

    /// authenticate method for the specified plugin
    async fn authenticate(
        &self,
        _auth_plugin: &str,
        username: &[u8],
        _salt: &[u8],
        _auth_data: &[u8],
    ) -> bool {
        println!("authenticating user {:?}", String::from_utf8_lossy(username));
        username == "test".as_bytes()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:3306").await?;

    loop {
        let (stream, _) = listener.accept().await?;
        let (r, w) = stream.into_split();
        tokio::spawn(async move { AsyncMysqlIntermediary::run_on(Backend, r, w).await });
    }
}
