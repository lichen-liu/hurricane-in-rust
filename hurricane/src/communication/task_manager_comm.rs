use crate::common::messages::{Task, TaskCompletedAck};
use bincode;
use byteorder::{BigEndian, ByteOrder};
use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio_io::codec;

/// Comm Message for `TaskManager`.
#[derive(Serialize, Deserialize)]
pub enum TaskManagerComm {
    /// Inform TaskManager that AppMaster is ready.
    ///
    /// `SocketAddr` - The sender of this msg.
    AppMasterIsReady(SocketAddr),

    /// Send TaskManager the task from AppMaster.
    AppMasterTask(Task),

    /// Ack TaskManager on task completed from AppMaster.
    AppMasterTaskCompletedAck(TaskCompletedAck),

    PoisonPill,
}

/// Codec for `TaskManagerComm` for TCP connection.
pub struct TaskManagerCommCodec;

impl codec::Decoder for TaskManagerCommCodec {
    type Item = TaskManagerComm;
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

impl codec::Encoder for TaskManagerCommCodec {
    type Item = TaskManagerComm;
    type Error = bincode::Error;

    fn encode(&mut self, msg: TaskManagerComm, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let msg = bincode::serialize(&msg)?;
        let msg_ref: &[u8] = msg.as_ref();

        dst.reserve(msg_ref.len() + 4);
        dst.put_u32_be(msg_ref.len() as u32);
        dst.put(msg_ref);

        Ok(())
    }
}
