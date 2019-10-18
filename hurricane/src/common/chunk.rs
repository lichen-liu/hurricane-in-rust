use super::bag::*;
use super::data_serializer::*;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::io::{Error, ErrorKind, Result};
use std::iter::Iterator;
use std::ops::Range;
use std::str;

pub mod metadata {
    #![allow(dead_code)]
    use crate::util::config::ChunkConfig;

    /// Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize
    static mut DATA_SIZE: usize = 1048576;
    /// Config.HurricaneConfig.BackendConfig.DataConfig.metaSize
    static mut META_SIZE: usize = unsafe { CHUNK_SIZE_SIZE + CMD_SIZE + BAG_SIZE };

    static mut SIZE: usize = unsafe { DATA_SIZE + META_SIZE };

    /// Config.HurricaneConfig.BackendConfig.DataConfig.chunkSizeSize
    static mut CHUNK_SIZE_SIZE: usize = 4;
    /// Config.HurricaneConfig.BackendConfig.DataConfig.cmdSize
    static mut CMD_SIZE: usize = 96;
    ///  Config.HurricaneConfig.BackendConfig.DataConfig.bagSize
    static mut BAG_SIZE: usize = 64;

    /// Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize
    static mut CHUNK_SIZE_OFFSET: usize = unsafe { DATA_SIZE };
    /// Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize + chunkSizeSize
    static mut CMD_OFFSET: usize = unsafe { CHUNK_SIZE_OFFSET + CHUNK_SIZE_SIZE };
    /// Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize + chunkSizeSize + cmdSize
    static mut BAG_OFFSET: usize = unsafe { CMD_OFFSET + CMD_SIZE };

    /// This function must be used in caution. Should only be used
    /// during initialization.
    pub fn set(config: ChunkConfig) {
        unsafe {
            DATA_SIZE = config.data_size;
            CHUNK_SIZE_SIZE = config.chunk_size_size;
            CMD_SIZE = config.cmd_size;
            BAG_SIZE = config.bag_size;
            META_SIZE = CHUNK_SIZE_SIZE + CMD_SIZE + BAG_SIZE;
            SIZE = DATA_SIZE + META_SIZE;
            CHUNK_SIZE_OFFSET = DATA_SIZE;
            CMD_OFFSET = CHUNK_SIZE_OFFSET + CHUNK_SIZE_SIZE;
            BAG_OFFSET = CMD_OFFSET + CMD_SIZE;
        }
    }

    /// Get the data region size of a chunk.
    pub fn data_size() -> usize {
        unsafe { DATA_SIZE }
    }

    /// Get the meta region size of a chunk.
    pub fn meta_size() -> usize {
        unsafe { META_SIZE }
    }

    /// Get the total size of a chunk.
    pub fn size() -> usize {
        unsafe { SIZE }
    }

    /// Get the size of data region size.
    pub fn chunk_size_size() -> usize {
        unsafe { CHUNK_SIZE_SIZE }
    }

    /// Get the command region size in meta region.
    pub fn cmd_size() -> usize {
        unsafe { CMD_SIZE }
    }

    /// Get the bag region size in meta region.
    pub fn bag_size() -> usize {
        unsafe { BAG_SIZE }
    }

    /// Get the offset to chunk size region.
    pub fn chunk_size_offset() -> usize {
        unsafe { CHUNK_SIZE_OFFSET }
    }

    /// Get the offset to command region.
    pub fn cmd_offset() -> usize {
        unsafe { CMD_OFFSET }
    }

    /// Get the offset to bag region.
    pub fn bag_offset() -> usize {
        unsafe { BAG_OFFSET }
    }
}

/// Trait to access the internal storage.
pub trait ChunkStorage {
    fn array_ref(&self) -> &[u8];
    fn array_mut(&mut self) -> &mut [u8];
    fn into_array(self) -> Box<[u8]>;
}

/// Trait to access the chunk meta data.
pub trait ChunkMeta: ChunkStorage {
    /// Get the actual data size inside the chunk.
    ///
    /// # Warning
    /// The chunk size must not exceed the data region size of the chunk.
    fn get_chunk_size(&self) -> usize {
        let chunk_size = Cursor::new(
            &(self.array_ref())[metadata::chunk_size_offset()
                ..metadata::chunk_size_offset() + metadata::chunk_size_size()],
        )
        .read_uint::<BigEndian>(metadata::chunk_size_size())
        .unwrap() as usize;

        assert!(chunk_size <= metadata::data_size());

        chunk_size
    }

    /// Set the actual data size inside the chunk.
    ///
    /// # Arguments
    /// * `chunk_size` - actual data size, must not exceed the data region size of the chunk.
    fn set_chunk_size(&mut self, chunk_size: usize) -> Result<()> {
        if chunk_size > metadata::data_size() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Chunk size exceeds data region size of a chunk.",
            ));
        }

        Cursor::new(
            &mut (self.array_mut())[metadata::chunk_size_offset()
                ..metadata::chunk_size_offset() + metadata::chunk_size_size()],
        )
        .write_uint::<BigEndian>(chunk_size as u64, metadata::chunk_size_size())
    }

    /// Get the associated command field from the chunk.
    fn as_cmd(&self) -> &[u8] {
        // source array
        let array = self.array_ref();
        &array[metadata::cmd_offset()..metadata::cmd_offset() + metadata::cmd_size()]
    }

    /// Get the associated mutable command field from the chunk.
    fn as_cmd_mut(&mut self) -> &mut [u8] {
        // source array
        let array = self.array_mut();
        &mut array[metadata::cmd_offset()..metadata::cmd_offset() + metadata::cmd_size()]
    }

    /// Set the associated `Bag` into the chunk.
    fn set_bag(&mut self, bag: &Bag) -> Result<()> {
        // target array
        let array = self.array_mut();
        let bag_array =
            &mut array[metadata::bag_offset()..metadata::bag_offset() + metadata::bag_size()];
        // Set the bag portion of array to 0.
        bag_array.iter_mut().for_each(|b| {
            *b = 0;
        });

        // source bytes
        let bag_bytes = bag.name.as_bytes();

        if bag_bytes.len() > metadata::bag_size() {
            // Not enough space to store bag.
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough space to store input Bag.",
            ));
        }

        // length to copy
        let bag_length = bag_bytes.len();

        let bag_dest = &mut bag_array[..bag_length];
        let bag_src = &bag_bytes[..bag_length];

        bag_dest.copy_from_slice(bag_src);

        return Ok(());
    }

    /// Get the associated `Bag` from the chunk.
    fn get_bag(&self) -> Bag {
        // source array
        let array = self.array_ref();
        let bag_array =
            &array[metadata::bag_offset()..metadata::bag_offset() + metadata::bag_size()];

        let bag_name = str::from_utf8(bag_array).expect("Bag ID must be valid UTF8 string.");
        let bag_name = bag_name.trim_matches(char::from(0));

        Bag::from(bag_name)
    }
}

/// Trait to access the chunk.
pub trait ChunkExt: ChunkMeta + ChunkStorage {
    /// The `Range` for which the buffer is located within the `Chunk`.
    fn get_buffer_range(&self) -> Range<usize> {
        0..self.get_chunk_size()
    }

    /// Get a `&[u8]` of the data section of the chunk.
    fn as_buffer_ref(&self) -> &[u8] {
        let chunk_size = self.get_chunk_size();
        &(self.array_ref())[0..chunk_size]
    }

    /// Get a `&mut [u8]` of the data section of the chunk.
    fn as_buffer_mut(&mut self) -> &mut [u8] {
        let chunk_size = self.get_chunk_size();
        &mut (self.array_mut())[0..chunk_size]
    }

    /// Consume `Chunk` into an `Iterator` over cloned data of the data section of the chunk.
    fn into_iter<F>(self) -> Box<dyn Iterator<Item = F::Item> + Send>
    where
        Self: Sized,
        F: 'static + Format,
    {
        let range = self.get_buffer_range();
        Box::new(F::into_iter(self.into_array(), range))
    }

    /// Get a `Pusher` of the data section of the chunk.
    fn into_pusher<F>(self) -> Box<dyn Pusher<Item = F::Item, InnerContainer = Buffer> + Send>
    where
        Self: Sized,
        F: 'static + Format,
    {
        let range = self.get_buffer_range();
        Box::new(F::into_pusher(self.into_array(), range))
    }
}

/// A chunk struct using `Box<[u8]>` as the internal storage.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Chunk {
    // Box<[u8]> type must be used as a whole from the very beginning, as
    // [u8] -> Box<[u8]> conversion involves copying [u8] from stack onto the heap.
    array: Box<[u8]>,
}

impl Chunk {
    /// Construct a `Chunk` with default size storage.
    pub fn new() -> Chunk {
        Chunk {
            array: vec![0; metadata::size()].into_boxed_slice(),
        }
    }

    pub fn from_pusher<P>(pusher: Box<P>) -> Chunk
    where
        P: Pusher<InnerContainer = Buffer> + Send + ?Sized,
    {
        let (pos, buf) = pusher.terminate_from_box();
        let mut chunk = Chunk::from_boxed_slice(buf);
        chunk.set_chunk_size(pos).unwrap();
        chunk
    }

    /// Construct a `Chunk` with argument boxed slice.
    ///
    /// The size of the argument boxed slice is checked.
    pub fn from_boxed_slice(array: Box<[u8]>) -> Chunk {
        assert!(array.len() >= metadata::size());
        Chunk { array }
    }

    /// Construct a `Chunk` with argument slice.
    ///
    /// The size of the argument slice is checked.
    pub fn from_slice(slice: &[u8]) -> Chunk {
        assert!(slice.len() >= metadata::size());
        Chunk {
            array: slice.to_vec().into_boxed_slice(),
        }
    }

    /// Construct a `Chunk` with argument vector.
    ///
    /// The size of the argument vector is checked.
    pub fn from_vec(vector: Vec<u8>) -> Chunk {
        assert!(vector.len() >= metadata::size());
        Chunk {
            array: vector.into_boxed_slice(),
        }
    }
}

impl ChunkStorage for Chunk {
    fn array_ref(&self) -> &[u8] {
        &(*self.array)
    }

    fn array_mut(&mut self) -> &mut [u8] {
        &mut (*self.array)
    }

    fn into_array(self) -> Box<[u8]> {
        self.array
    }
}

impl ChunkMeta for Chunk {}

impl ChunkExt for Chunk {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::primitive_serializer::*;
    use std::io::Write;

    #[test]
    fn chunk_initialization() {
        let array1 = [0, 1, 2, 3, 4, 5];

        // Note: using unchecked Chunk initialization,
        // since array1 does not have a valid size.
        let chunk1 = Chunk {
            array: Box::new(array1),
        };
        let chunk2 = Chunk {
            array: Box::new(array1),
        };

        assert_eq!(chunk1.array, chunk2.array);
    }

    #[test]
    fn chunk_constructors() {
        let chunk1 = Chunk::new();
        assert_eq!(chunk1.array_ref().len(), metadata::size());

        let chunk2 = Chunk::from_boxed_slice(vec![0; metadata::size() + 1].into_boxed_slice());
        assert_eq!(chunk2.array_ref().len(), metadata::size() + 1);

        let chunk3 = Chunk::from_vec(vec![0; metadata::size() + 2]);
        assert_eq!(chunk3.array_ref().len(), metadata::size() + 2);
    }

    #[test]
    #[should_panic]
    fn chunk_invalid_constructor() {
        let _chunk = Chunk::from_vec(vec![0; metadata::size() - 1]);
    }

    #[test]
    fn chunk_storage() {
        // Note: using unchecked Chunk initialization,
        // since array does not have a valid size.
        let mut chunk = Chunk {
            array: Box::new([0, 1, 2, 3, 4]),
        };

        assert_eq!(chunk.array_ref(), [0, 1, 2, 3, 4]);

        let chunk_array_mut = chunk.array_mut();
        chunk_array_mut[0] = 5;
        chunk_array_mut[1] = 6;
        chunk_array_mut[2] = 7;
        chunk_array_mut[3] = 8;
        chunk_array_mut[4] = 9;
        assert_eq!(chunk.array_ref(), [5, 6, 7, 8, 9]);
    }

    #[test]
    fn chunk_chunk_size() {
        let mut chunk = Chunk::new();

        chunk.set_chunk_size(48).unwrap();
        assert_eq!(chunk.get_chunk_size(), 48);
    }

    #[test]
    fn chunk_invalid_chunk_size() -> std::result::Result<(), String> {
        let mut chunk = Chunk::new();

        match chunk.set_chunk_size(metadata::data_size() + 1) {
            Err(_) => Ok(()),
            _ => Err(String::from(
                "Test did not fail for invalid chunk size input.",
            )),
        }
    }

    #[test]
    fn chunk_buffer_view() {
        let mut chunk = Chunk::new();

        chunk.set_chunk_size(64).unwrap();

        let buffer_mut = chunk.as_buffer_mut();

        for i in 0 as u8..16 as u8 {
            buffer_mut[(i as usize) * 4] = i;
            buffer_mut[(i as usize) * 4 + 1] = i;
            buffer_mut[(i as usize) * 4 + 2] = i;
            buffer_mut[(i as usize) * 4 + 3] = i;
        }

        let mut buffer = Cursor::new(chunk.as_buffer_ref());
        for i in 0 as u8..16 as u8 {
            assert_eq!(
                buffer.read_u32::<BigEndian>().unwrap(),
                (i as u32) << 24 | (i as u32) << 16 | (i as u32) << 8 | (i as u32)
            );
        }
    }

    #[test]
    fn chunk_iterator_i32format() {
        let mut chunk = Chunk::new();

        chunk.set_chunk_size(64).unwrap();

        let buffer_mut = chunk.as_buffer_mut();

        for i in 0 as u8..16 as u8 {
            buffer_mut[(i as usize) * 4] = i;
            buffer_mut[(i as usize) * 4 + 1] = i;
            buffer_mut[(i as usize) * 4 + 2] = i;
            buffer_mut[(i as usize) * 4 + 3] = i;
        }

        let mut it = chunk.into_iter::<I32Format>();
        for i in 0 as u8..16 as u8 {
            assert_eq!(
                it.next(),
                Some((i as i32) << 24 | (i as i32) << 16 | (i as i32) << 8 | (i as i32))
            );
        }

        assert_eq!(it.next(), None);
    }

    #[test]
    fn chunk_pusher_i32format() {
        let mut chunk = Chunk::new();

        chunk.set_chunk_size(64).unwrap();

        let mut pusher = chunk.into_pusher::<I32Format>();
        for i in 0 as u8..16 as u8 {
            assert_eq!(
                pusher.put((i as i32) << 24 | (i as i32) << 16 | (i as i32) << 8 | (i as i32)),
                Some(())
            );
        }

        assert_eq!(pusher.put(0), None);

        let chunk = Chunk::from_boxed_slice(pusher.into_inner_from_box());
        let buffer = chunk.as_buffer_ref();

        for i in 0 as u8..16 as u8 {
            assert_eq!(buffer[(i as usize) * 4], i);
            assert_eq!(buffer[(i as usize) * 4 + 1], i);
            assert_eq!(buffer[(i as usize) * 4 + 2], i);
            assert_eq!(buffer[(i as usize) * 4 + 3], i);
        }
    }

    #[test]
    fn chunk_pusher_iterator_spacedstringformat() {
        let mut chunk = Chunk::new();

        chunk.set_chunk_size(22).unwrap();

        let mut pusher = chunk.into_pusher::<SpacedStringFormat>();
        assert_eq!(Some(()), pusher.put(String::from("Hello")));
        assert_eq!(Some(()), pusher.put(String::from("World")));
        assert_eq!(Some(()), pusher.put(String::from("From")));
        assert_eq!(Some(()), pusher.put(String::from("Rust")));

        let chunk = Chunk::from_pusher(pusher);

        let mut it = chunk.into_iter::<SpacedStringFormat>();
        assert_eq!(it.next(), Some(String::from("Hello")));
        assert_eq!(it.next(), Some(String::from("World")));
        assert_eq!(it.next(), Some(String::from("From")));
        assert_eq!(it.next(), Some(String::from("Rust")));
        assert_eq!(it.next(), None);
    }

    #[test]
    fn chunk_cmd() {
        let mut chunk = Chunk::new();

        chunk.set_chunk_size(64).unwrap();

        chunk.as_cmd_mut().write_all(b"DESTROY EARTH").unwrap();
        assert_eq!(
            str::from_utf8(chunk.as_cmd())
                .unwrap()
                .trim_matches(char::from(0)),
            "DESTROY EARTH"
        );
    }

    #[test]
    fn chunk_bag() {
        let mut chunk = Chunk::new();

        chunk.set_chunk_size(64).unwrap();

        let bag = Bag::from("BAG-1");
        chunk.set_bag(&bag).unwrap();
        assert_eq!(bag, chunk.get_bag());
    }
}
