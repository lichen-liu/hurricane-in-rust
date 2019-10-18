use crate::common::bag::Bag;
use crate::common::chunk::*;
use crate::common::types::*;
use bincode;
use byteorder::{BigEndian, ByteOrder};
use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::net::SocketAddr;
use tokio_io::codec;

/// Comm Message for `HurricaneWorkIO`.
#[derive(Serialize, Deserialize)]
pub enum HurricaneWorkIOComm {
    /// Ack for receiving the previously `HurricaneIOComm::Drain` request.
    AckDrain(SocketAddr, FingerPrint, Bag),

    /// The progress report for `Bag`.
    ///
    /// (Bytes Done, Bytes Total)
    ProgressReport(SocketAddr, TaskId, FingerPrint, Bag, usize, usize),

    /// The filled `Chunk` for `Bag`.
    Filled(SocketAddr, FingerPrint, Bag, Chunk),

    /// Similiar to `Filled`, but the content of `Chunk` is not involved.
    /// This message serves as a light weight version of `Filled`,
    /// as no `Chunk` data is transfered, it, rather, transfers the ownership
    /// of `Chunk` to frontend, if the bag is not yet EOFed.
    FakeFilled(SocketAddr, FingerPrint, Bag, usize),

    /// A signal indicating `Bag` has been fully consumed on
    /// the sender `HurricaneIO`.
    EOF(SocketAddr, FingerPrint, Bag),
}

/// Tag for `HurricaneWorkIOComm`.
#[derive(Serialize, Deserialize)]
pub enum HurricaneWorkIOCommTag {
    Chunk,
    NoChunk,
}

/// Codec for `HurricaneWorkIOComm` for TCP connection.
/// Hand-written codec, with two mode support.
/// 1) If the message contains a chunk, then the message is stored within the chunk.
/// 2) If the message does not contain a chunk, then encode as usual.
/// Doing so can save a cloning of `Chunk` by the external serializer.
/// I.e. serializing the message with the chunk into a buffer first (involving
/// 1 copying of chunk), and then copying the buffer into dest buffer (involving
/// another copying of chunk).
///
/// As the most frequently used message for this Comm is `Filled`, optimization
/// on avoiding copying chunks is a high priority.
pub struct HurricaneWorkIOCommCodec;

impl codec::Decoder for HurricaneWorkIOCommCodec {
    type Item = HurricaneWorkIOComm;
    type Error = bincode::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 + 4 {
            return Ok(None);
        }

        let tag_size = BigEndian::read_u32(&src[0..4]) as usize;
        let msg_size = BigEndian::read_u32(&src[4..8]) as usize;

        if src.len() < 4 + 4 + tag_size + msg_size {
            return Ok(None);
        }

        // Skip size fields.
        src.split_to(4 + 4);

        // Split.
        let tag_buf = src.split_to(tag_size);
        let msg_buf = src.split_to(msg_size);

        let msg_out = match bincode::deserialize(&tag_buf)? {
            HurricaneWorkIOCommTag::Chunk => {
                let chunk = Chunk::from_slice(&msg_buf);
                let mut chunk_cmd = Cursor::new(chunk.as_cmd());

                let sender = bincode::deserialize_from(&mut chunk_cmd)?;
                let fingerprint = bincode::deserialize_from(&mut chunk_cmd)?;
                let bag = chunk.get_bag();

                HurricaneWorkIOComm::Filled(sender, fingerprint, bag, chunk)
            }
            HurricaneWorkIOCommTag::NoChunk => bincode::deserialize(&msg_buf)?,
        };

        Ok(Some(msg_out))
    }
}

impl codec::Encoder for HurricaneWorkIOCommCodec {
    type Item = HurricaneWorkIOComm;
    type Error = bincode::Error;

    fn encode(&mut self, msg: HurricaneWorkIOComm, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if let HurricaneWorkIOComm::Filled(sender, fingerprint, bag, mut chunk) = msg {
            let mut chunk_cmd = Cursor::new(chunk.as_cmd_mut());
            bincode::serialize_into(&mut chunk_cmd, &sender)?;
            bincode::serialize_into(&mut chunk_cmd, &fingerprint)?;
            chunk.set_bag(&bag)?;

            let msg_tag_bin = bincode::serialize(&HurricaneWorkIOCommTag::Chunk)?;

            let chunk_array_ref = chunk.array_ref();
            let msg_tag_bin_ref: &[u8] = msg_tag_bin.as_ref();

            let tag_bin_size = msg_tag_bin_ref.len();
            let msg_bin_size = chunk_array_ref.len();

            dst.reserve(tag_bin_size + msg_bin_size + 4 + 4);
            // Put tag size first.
            dst.put_u32_be(tag_bin_size as u32);
            // Put msg size then.
            dst.put_u32_be(msg_bin_size as u32);
            // Put tag.
            dst.put(msg_tag_bin_ref);
            // Put msg.
            dst.put(chunk_array_ref);
        } else {
            let msg_bin = bincode::serialize(&msg)?;
            let msg_tag_bin = bincode::serialize(&HurricaneWorkIOCommTag::NoChunk)?;
            let msg_bin_ref: &[u8] = msg_bin.as_ref();
            let msg_tag_bin_ref: &[u8] = msg_tag_bin.as_ref();

            let tag_bin_size = msg_tag_bin_ref.len();
            let msg_bin_size = msg_bin_ref.len();

            dst.reserve(tag_bin_size + msg_bin_size + 4 + 4);
            // Put tag size first.
            dst.put_u32_be(tag_bin_size as u32);
            // Put msg size first.
            dst.put_u32_be(msg_bin_size as u32);
            // Put tag.
            dst.put(msg_tag_bin_ref);
            // Put msg.
            dst.put(msg_bin_ref);
        }

        Ok(())
    }
}
