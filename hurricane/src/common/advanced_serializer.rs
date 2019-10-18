use super::data_serializer::*;
use super::primitive_serializer::PrimitiveFormatPusher;
use bit_vec::*;
use std::io::Cursor;
use std::io::{Read, Write};
use std::ops::Range;
use std::vec::IntoIter;

/// Static object instance of specialized `BitVecFormat`.
pub static BITVEC_FORMATTER: BitVecFormat = BitVecFormat;

pub struct BitVecFormat;

impl Endian for BitVecFormat {}

impl DataSerializer for BitVecFormat {
    type Item = BitVec;

    fn to_binary(buf: &mut BufferViewMut, bv: BitVec) -> Option<()> {
        let bv_bytes = bv.to_bytes();
        assert_eq!(bv_bytes.len(), buf.get_ref().len());
        buf.write_all(&bv_bytes).ok()
    }

    fn from_binary(buf: &mut BufferView) -> Option<BitVec> {
        let num_bytes = buf.get_ref().len();
        let mut bv_vec = vec![0; num_bytes];
        buf.read_exact(&mut bv_vec)
            .ok()
            .map(move |()| BitVec::from_bytes(&bv_vec))
    }
}

impl Format for BitVecFormat {
    type Iter = IntoIter<BitVec>;
    type Pusher = PrimitiveFormatPusher<Self>;

    fn into_iter(buf: Buffer, range: Range<usize>) -> IntoIter<BitVec> {
        let mut buf_view: BufferView = Cursor::new(&buf[range]);
        Self::reads(&mut buf_view).map_or(Vec::new().into_iter(), |bv| vec![bv].into_iter())
    }

    fn into_pusher(buf: Buffer, range: Range<usize>) -> PrimitiveFormatPusher<Self> {
        PrimitiveFormatPusher::new(buf, range)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pusher_iterator_small_bitvecformat() {
        let num_bytes = 4;
        let buf: Buffer = vec![0 as u8; num_bytes].into_boxed_slice();
        let range = 0..buf.len();
        let mut pu = BitVecFormat::into_pusher(buf, range);

        let mut bv = BitVec::from_elem(num_bytes * 8, true);
        bv.set(15, false);
        assert_eq!(Some(()), pu.put(bv.clone()));;
        assert_eq!(None, pu.put(bv.clone()));

        let buf = pu.into_inner();
        let range = 0..buf.len();
        let mut it = BitVecFormat::into_iter(buf, range);

        assert_eq!(Some(bv), it.next());
        assert_eq!(None, it.next());
    }

    #[test]
    #[should_panic]
    fn pusher_small_bitvecformat_unmatched_size() {
        let num_bytes = 4;
        let buf: Buffer = vec![0 as u8; num_bytes].into_boxed_slice();
        let range = 0..buf.len();
        let mut pu = BitVecFormat::into_pusher(buf, range);

        let mut bv = BitVec::from_elem(num_bytes * 8 + 1, true);
        bv.set(15, false);
        assert_eq!(Some(()), pu.put(bv.clone()));;
    }

    #[test]
    fn pusher_iterator_large_bitvecformat() {
        let num_bytes = 1 << 22;
        let buf: Buffer = vec![0 as u8; num_bytes].into_boxed_slice();
        let range = 0..buf.len();
        let mut pu = BitVecFormat::into_pusher(buf, range);

        let bv = BitVec::from_elem(num_bytes * 8, false);
        let bv: BitVec = bv.into_iter().map(|_| rand::random()).collect();
        assert_eq!(Some(()), pu.put(bv.clone()));;
        assert_eq!(None, pu.put(bv.clone()));

        let buf = pu.into_inner();
        let range = 0..buf.len();
        let mut it = BitVecFormat::into_iter(buf, range);

        assert_eq!(Some(bv), it.next());
        assert_eq!(None, it.next());
    }
}
