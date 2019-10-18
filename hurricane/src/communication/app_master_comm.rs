use crate::common::types::TaskId;
use bincode;
use byteorder::{BigEndian, ByteOrder};
use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio_io::codec;

/// Comm Message for `AppMaster`.
#[derive(Serialize, Deserialize)]
pub enum AppMasterComm {
    /// Inform AppMaster that TaskManager is requesting a task.
    ///
    /// `SocketAddr` - The sender of this msg.
    TaskManagerGetTask(SocketAddr),

    /// Inform AppMaster that TaskManager has completed the task.
    ///
    /// `SocketAddr` - The sender of this msg.
    TaskManagerTaskCompleted(SocketAddr, TaskId),

    /// Inform AppMaster that TaskManager is requesting to clone task.
    ///
    /// `SocketAddr` - The sender of this msg.
    TaskManagerPleaseClone(SocketAddr, TaskId),
}

/// Codec for `AppMasterComm` for TCP connection.
pub struct AppMasterCommCodec;

impl codec::Decoder for AppMasterCommCodec {
    type Item = AppMasterComm;
    type Error = bincode::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let size = {
            if src.len() < 4 {
                return Ok(None);
            }
            BigEndian::read_u32(src.as_ref()) as usize
        };

        if src.len() >= size + 4 {
            src.split_to(4);
            let buf = src.split_to(size);
            Ok(Some(bincode::deserialize(&buf)?))
        } else {
            Ok(None)
        }
    }
}

impl codec::Encoder for AppMasterCommCodec {
    type Item = AppMasterComm;
    type Error = bincode::Error;

    fn encode(&mut self, msg: AppMasterComm, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let msg = bincode::serialize(&msg)?;
        let msg_ref: &[u8] = msg.as_ref();

        dst.reserve(msg_ref.len() + 4);
        dst.put_u32_be(msg_ref.len() as u32);
        dst.put(msg_ref);

        Ok(())
    }
}
