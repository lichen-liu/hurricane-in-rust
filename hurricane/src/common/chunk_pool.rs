use super::chunk::*;

/// Factory for chunk.
pub struct ChunkPool;

impl ChunkPool {
    /// Allocate a chunk.
    pub fn allocate() -> Chunk {
        let mut ret = Chunk::new();

        ret.set_chunk_size(metadata::data_size()).unwrap();

        ret
    }

    /// Allocate a chunk filled with `Some(size)` many of `u8` elements
    /// with `content`. If `None` is supplied, then filled till its capacity.
    pub fn allocate_filled_u8(content: u8, size: Option<usize>) -> Chunk {
        let mut chunk = Chunk::new();

        let size = size.unwrap_or(metadata::data_size());
        assert!(size <= metadata::data_size());
        chunk.set_chunk_size(size).unwrap();

        chunk.as_buffer_mut().iter_mut().for_each(|b| *b = content);

        chunk
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunkpool_allocate() {
        let c = ChunkPool::allocate();

        assert_eq!(metadata::data_size(), c.get_chunk_size());
    }
}
