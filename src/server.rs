use crate::error::{Error, Result};
use crate::sql;
use crate::sql::engine::{Engine as _, Mode};
use crate::sql::execution::ResultSet;
use crate::sql::schema::{Catalog as _, Table};
use crate::sql::types::Row;
use crate::storage::kv;

use futures::sink::SinkExt as _;
use log::{error, info};
use serde_derive::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::StreamExt as _;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub struct Server {
    engine: sql::engine::KV,
    listener: Option<TcpListener>,
}

impl Server {
    pub async fn new(store: Box<dyn kv::Store>) -> Result<Self> {
        let mvcc = kv::MVCC::new(store);
        let engine = sql::engine::KV::new(mvcc);
        Ok(Self {
            engine,
            listener: None,
        })
    }

    pub async fn listen(mut self, addr: &str) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        self.listener = Some(listener);
        Ok(self)
    }

    pub async fn serve(self) -> Result<()> {
        let listener = self
            .listener
            .ok_or_else(|| Error::Internal("Must listen before serving".into()))?;

        Self::serve_sql(listener, self.engine).await?;

        Ok(())
    }

    async fn serve_sql(listener: TcpListener, engine: sql::engine::KV) -> Result<()> {
        let mut listener = TcpListenerStream::new(listener);
        while let Some(socket) = listener.try_next().await? {
            let peer = socket.peer_addr()?;
            let session = Session::new(engine.clone())?;
            tokio::spawn(async move {
                info!("Client {} connected", peer);
                match session.handle(socket).await {
                    Ok(()) => info!("Client {} disconnected", peer),
                    Err(err) => error!("Client {} error: {}", peer, err),
                }
            });
        }
        Ok(())
    }
}

/// 客户端请求
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Execute(String),
    GetTable(String),
    ListTables,
    // Status,
}

/// 服务端响应
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Execute(ResultSet),
    Row(Option<Row>),
    GetTable(Table),
    ListTables(Vec<String>),
    // Status(sql::engine::Status),
}

/// 与 SQL 会话耦合的客户端会话。
pub struct Session {
    _engine: sql::engine::KV,
    sql: sql::engine::Session<sql::engine::KV>,
}

impl Session {
    /// Create a new client session.
    fn new(engine: sql::engine::KV) -> Result<Self> {
        Ok(Self {
            sql: engine.session()?,
            _engine: engine,
        })
    }

    /// Handles a client connection.
    async fn handle(mut self, socket: TcpStream) -> Result<()> {
        let mut stream = tokio_serde::Framed::new(
            Framed::new(socket, LengthDelimitedCodec::new()),
            tokio_serde::formats::Bincode::default(),
        );

        while let Some(request) = stream.try_next().await? {
            let mut response = tokio::task::block_in_place(|| self.request(request));
            let mut rows: Box<dyn Iterator<Item = Result<Response>> + Send> =
                Box::new(std::iter::empty());

            if let Ok(Response::Execute(ResultSet::Query {
                rows: ref mut resultrows,
                ..
            })) = &mut response
            {
                rows = Box::new(
                    std::mem::replace(resultrows, Box::new(std::iter::empty()))
                        .map(|result| result.map(|row| Response::Row(Some(row))))
                        .chain(std::iter::once(Ok(Response::Row(None))))
                        .scan(false, |err_sent, response| match (&err_sent, &response) {
                            (true, _) => None,
                            (_, Err(error)) => {
                                *err_sent = true;
                                Some(Err(error.clone()))
                            }
                            _ => Some(response),
                        })
                        .fuse(),
                );
            }
            stream.send(response).await?;
            stream
                .send_all(&mut tokio_stream::iter(rows.map(Ok)))
                .await?;
        }

        Ok(())
    }

    pub fn request(&mut self, request: Request) -> Result<Response> {
        Ok(match request {
            Request::Execute(query) => Response::Execute(self.sql.execute(&query)?),
            Request::GetTable(table) => Response::GetTable(
                self.sql
                    .with_txn(Mode::ReadOnly, |txn| txn.must_read_table(&table))?,
            ),
            Request::ListTables => {
                Response::ListTables(self.sql.with_txn(Mode::ReadOnly, |txn| {
                    Ok(txn.tables()?.map(|t| t.name).collect())
                })?)
            } // Request::Status => Response::Status(self.engine.status()?),
        })
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        tokio::task::block_in_place(|| self.sql.execute("ROLLBACK").ok());
    }
}
