use super::data_serializer::*;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;
use std::io::{Read, Write};
use std::iter::Iterator;
use std::marker::PhantomData;
use std::ops::Range;
use std::vec::IntoIter;

/// Specialized `PrimitiveFormat<T>`.
pub type I32Format = PrimitiveFormat<i32>;
/// Specialized `PrimitiveFormat<T>`.
pub type I64Format = PrimitiveFormat<i64>;
/// Specialized `PrimitiveFormat<T>`.
pub type I16Format = PrimitiveFormat<i16>;
/// Specialized `PrimitiveFormat<T>`.
pub type F32Format = PrimitiveFormat<f32>;
/// Specialized `PrimitiveFormat<T>`.
pub type F64Format = PrimitiveFormat<f64>;
/// Specialized `PrimitiveFormat<T>`.
pub type U8Format = PrimitiveFormat<u8>;
/// Specialized `PrimitiveFormat<T>`.
pub type CharFormat = PrimitiveFormat<char>;
/// Specialized `PrimitiveFormat<T>`.
pub type I32I32Format = PrimitiveFormat<(i32, i32)>;
/// Specialized `StringFormat<T>`.
pub type SpacedStringFormat = StringFormat<SpaceSeparator>;
/// Specialized `StringFormat<T>`.
pub type LineByLineStringFormat = StringFormat<NewLineSeparator>;

/// Static object instance of specialized `PrimitiveFormat<T>`.
pub static I32_FORMATTER: I32Format = PrimitiveFormat(PhantomData);
/// Static object instance of specialized `PrimitiveFormat<T>`.
pub static I64_FORMATTER: I64Format = PrimitiveFormat(PhantomData);
/// Static object instance of specialized `PrimitiveFormat<T>`.
pub static I16_FORMATTER: I16Format = PrimitiveFormat(PhantomData);
/// Static object instance of specialized `PrimitiveFormat<T>`.
pub static F32_FORMATTER: F32Format = PrimitiveFormat(PhantomData);
/// Static object instance of specialized `PrimitiveFormat<T>`.
pub static F64_FORMATTER: F64Format = PrimitiveFormat(PhantomData);
/// Static object instance of specialized `PrimitiveFormat<T>`.
pub static U8_FORMATTER: U8Format = PrimitiveFormat(PhantomData);
/// Static object instance of specialized `PrimitiveFormat<T>`.
pub static CHAR_FORMATTER: CharFormat = PrimitiveFormat(PhantomData);
/// Static object instance of specialized `PrimitiveFormat<T>`.
pub static I32I32_FORMATTER: I32I32Format = PrimitiveFormat(PhantomData);
/// Static object instance of specialized `StringFormat<T>`.
pub static SPACED_STRING_FORMATTER: SpacedStringFormat = StringFormat(PhantomData);
/// Static object instance of specialized `StringFormat<T>`.
pub static LINE_BY_LINE_STRING_FORMATTER: LineByLineStringFormat = StringFormat(PhantomData);

/// Iterator for primitive data type over a byte buffer.
pub struct PrimitiveFormatIter<T: Format> {
    buf: Buffer,
    phantom: PhantomData<T>,
    cursor_pos: u64,
    range: Range<usize>,
}

impl<T: Format> PrimitiveFormatIter<T> {
    pub fn new(buf: Buffer, range: Range<usize>) -> PrimitiveFormatIter<T> {
        PrimitiveFormatIter {
            buf,
            phantom: PhantomData,
            cursor_pos: 0,
            range,
        }
    }
}

impl<T: Format> Iterator for PrimitiveFormatIter<T> {
    type Item = T::Item;

    fn next(&mut self) -> Option<T::Item> {
        let mut buf_view: BufferView = Cursor::new(&self.buf[self.range.clone()]);
        buf_view.set_position(self.cursor_pos);
        let r = T::reads(&mut buf_view);
        self.cursor_pos = buf_view.position();
        r
    }
}

/// Pusher for primitive data type over a byte buffer.
pub struct PrimitiveFormatPusher<T: Format> {
    buf: Buffer,
    phantom: PhantomData<T>,
    cursor_pos: u64,
    range: Range<usize>,
}

impl<T: Format> PrimitiveFormatPusher<T> {
    pub fn new(buf: Buffer, range: Range<usize>) -> PrimitiveFormatPusher<T> {
        PrimitiveFormatPusher {
            buf,
            phantom: PhantomData,
            cursor_pos: 0,
            range,
        }
    }
}

impl<T: Format> Pusher for PrimitiveFormatPusher<T> {
    type Item = T::Item;
    type InnerContainer = Buffer;

    fn put(&mut self, something: Self::Item) -> Option<()> {
        let mut buf_view_mut: BufferViewMut = Cursor::new(&mut self.buf[self.range.clone()]);
        buf_view_mut.set_position(self.cursor_pos);
        let r = T::writes(&mut buf_view_mut, something);
        self.cursor_pos = buf_view_mut.position();
        r
    }

    fn terminate(self) -> (usize, Buffer) {
        (self.cursor_pos as usize, self.buf)
    }

    fn terminate_from_box(self: Box<Self>) -> (usize, Buffer) {
        (self.cursor_pos as usize, self.buf)
    }
}

/// Pusher for string type over a byte buffer.
///
/// The pusher atomatically appends a seperator character to the buffer for each put().
pub struct StringFormatPusher<T: Format + StringSeparator> {
    buf: Buffer,
    phantom: PhantomData<T>,
    cursor_pos: u64,
    range: Range<usize>,
}

impl<T: Format + StringSeparator> StringFormatPusher<T> {
    pub fn new(buf: Buffer, range: Range<usize>) -> StringFormatPusher<T> {
        StringFormatPusher {
            buf,
            phantom: PhantomData,
            cursor_pos: 0,
            range,
        }
    }
}

impl<T: Format + StringSeparator> Pusher for StringFormatPusher<T> {
    type Item = T::Item;
    type InnerContainer = Buffer;

    fn put(&mut self, something: Self::Item) -> Option<()> {
        let mut buf_view_mut: BufferViewMut = Cursor::new(&mut self.buf[self.range.clone()]);
        buf_view_mut.set_position(self.cursor_pos);

        let r = if let None = T::writes(&mut buf_view_mut, something) {
            None
        } else {
            CharFormat::writes(&mut buf_view_mut, T::SEPARATOR)
        };

        self.cursor_pos = buf_view_mut.position();
        r
    }

    fn terminate(self) -> (usize, Buffer) {
        (self.cursor_pos as usize, self.buf)
    }

    fn terminate_from_box(self: Box<Self>) -> (usize, Buffer) {
        (self.cursor_pos as usize, self.buf)
    }
}

/// Primitive data type format.
pub struct PrimitiveFormat<P>(PhantomData<P>);

impl Endian for PrimitiveFormat<i32> {}

impl DataSerializer for PrimitiveFormat<i32> {
    type Item = i32;

    fn to_binary(buf: &mut BufferViewMut, i: i32) -> Option<()> {
        if Self::is_big_endian() {
            buf.write_i32::<BigEndian>(i).ok()
        } else {
            buf.write_i32::<LittleEndian>(i).ok()
        }
    }

    fn from_binary(buf: &mut BufferView) -> Option<i32> {
        if Self::is_big_endian() {
            buf.read_i32::<BigEndian>().ok()
        } else {
            buf.read_i32::<LittleEndian>().ok()
        }
    }
}

impl Format for PrimitiveFormat<i32> {
    type Iter = PrimitiveFormatIter<Self>;
    type Pusher = PrimitiveFormatPusher<Self>;

    fn into_iter(buf: Buffer, range: Range<usize>) -> PrimitiveFormatIter<Self> {
        PrimitiveFormatIter::new(buf, range)
    }

    fn into_pusher(buf: Buffer, range: Range<usize>) -> PrimitiveFormatPusher<Self> {
        PrimitiveFormatPusher::new(buf, range)
    }
}

impl Endian for PrimitiveFormat<i64> {}

impl DataSerializer for PrimitiveFormat<i64> {
    type Item = i64;

    fn to_binary(buf: &mut BufferViewMut, l: i64) -> Option<()> {
        if Self::is_big_endian() {
            buf.write_i64::<BigEndian>(l).ok()
        } else {
            buf.write_i64::<LittleEndian>(l).ok()
        }
    }

    fn from_binary(buf: &mut BufferView) -> Option<i64> {
        if Self::is_big_endian() {
            buf.read_i64::<BigEndian>().ok()
        } else {
            buf.read_i64::<LittleEndian>().ok()
        }
    }
}

impl Format for PrimitiveFormat<i64> {
    type Iter = PrimitiveFormatIter<Self>;
    type Pusher = PrimitiveFormatPusher<Self>;

    fn into_iter(buf: Buffer, range: Range<usize>) -> PrimitiveFormatIter<Self> {
        PrimitiveFormatIter::new(buf, range)
    }

    fn into_pusher(buf: Buffer, range: Range<usize>) -> PrimitiveFormatPusher<Self> {
        PrimitiveFormatPusher::new(buf, range)
    }
}

impl Endian for PrimitiveFormat<i16> {}

impl DataSerializer for PrimitiveFormat<i16> {
    type Item = i16;

    fn to_binary(buf: &mut BufferViewMut, s: i16) -> Option<()> {
        if Self::is_big_endian() {
            buf.write_i16::<BigEndian>(s).ok()
        } else {
            buf.write_i16::<LittleEndian>(s).ok()
        }
    }

    fn from_binary(buf: &mut BufferView) -> Option<i16> {
        if Self::is_big_endian() {
            buf.read_i16::<BigEndian>().ok()
        } else {
            buf.read_i16::<LittleEndian>().ok()
        }
    }
}

impl Format for PrimitiveFormat<i16> {
    type Iter = PrimitiveFormatIter<Self>;
    type Pusher = PrimitiveFormatPusher<Self>;

    fn into_iter(buf: Buffer, range: Range<usize>) -> PrimitiveFormatIter<Self> {
        PrimitiveFormatIter::new(buf, range)
    }

    fn into_pusher(buf: Buffer, range: Range<usize>) -> PrimitiveFormatPusher<Self> {
        PrimitiveFormatPusher::new(buf, range)
    }
}

impl Endian for PrimitiveFormat<f32> {}

impl DataSerializer for PrimitiveFormat<f32> {
    type Item = f32;

    fn to_binary(buf: &mut BufferViewMut, f: f32) -> Option<()> {
        if Self::is_big_endian() {
            buf.write_f32::<BigEndian>(f).ok()
        } else {
            buf.write_f32::<LittleEndian>(f).ok()
        }
    }

    fn from_binary(buf: &mut BufferView) -> Option<f32> {
        if Self::is_big_endian() {
            buf.read_f32::<BigEndian>().ok()
        } else {
            buf.read_f32::<LittleEndian>().ok()
        }
    }
}

impl Format for PrimitiveFormat<f32> {
    type Iter = PrimitiveFormatIter<Self>;
    type Pusher = PrimitiveFormatPusher<Self>;

    fn into_iter(buf: Buffer, range: Range<usize>) -> PrimitiveFormatIter<Self> {
        PrimitiveFormatIter::new(buf, range)
    }

    fn into_pusher(buf: Buffer, range: Range<usize>) -> PrimitiveFormatPusher<Self> {
        PrimitiveFormatPusher::new(buf, range)
    }
}

impl Endian for PrimitiveFormat<f64> {}

impl DataSerializer for PrimitiveFormat<f64> {
    type Item = f64;

    fn to_binary(buf: &mut BufferViewMut, d: f64) -> Option<()> {
        if Self::is_big_endian() {
            buf.write_f64::<BigEndian>(d).ok()
        } else {
            buf.write_f64::<LittleEndian>(d).ok()
        }
    }

    fn from_binary(buf: &mut BufferView) -> Option<f64> {
        if Self::is_big_endian() {
            buf.read_f64::<BigEndian>().ok()
        } else {
            buf.read_f64::<LittleEndian>().ok()
        }
    }
}

impl Format for PrimitiveFormat<f64> {
    type Iter = PrimitiveFormatIter<Self>;
    type Pusher = PrimitiveFormatPusher<Self>;

    fn into_iter(buf: Buffer, range: Range<usize>) -> PrimitiveFormatIter<Self> {
        PrimitiveFormatIter::new(buf, range)
    }

    fn into_pusher(buf: Buffer, range: Range<usize>) -> PrimitiveFormatPusher<Self> {
        PrimitiveFormatPusher::new(buf, range)
    }
}

impl Endian for PrimitiveFormat<u8> {}

impl DataSerializer for PrimitiveFormat<u8> {
    type Item = u8;

    fn to_binary(buf: &mut BufferViewMut, b: u8) -> Option<()> {
        buf.write_u8(b).ok()
    }

    fn from_binary(buf: &mut BufferView) -> Option<u8> {
        buf.read_u8().ok()
    }
}

impl Format for PrimitiveFormat<u8> {
    type Iter = PrimitiveFormatIter<Self>;
    type Pusher = PrimitiveFormatPusher<Self>;

    fn into_iter(buf: Buffer, range: Range<usize>) -> PrimitiveFormatIter<Self> {
        PrimitiveFormatIter::new(buf, range)
    }

    fn into_pusher(buf: Buffer, range: Range<usize>) -> PrimitiveFormatPusher<Self> {
        PrimitiveFormatPusher::new(buf, range)
    }
}

impl Endian for PrimitiveFormat<char> {}

impl DataSerializer for PrimitiveFormat<char> {
    type Item = char;

    fn to_binary(buf: &mut BufferViewMut, c: char) -> Option<()> {
        assert_eq!(c.len_utf8(), 1, "Only support ASCII characters.");
        let mut ascii_buf: [u8; 1] = [0];
        c.encode_utf8(&mut ascii_buf);
        buf.write_u8(ascii_buf[0]).ok()
    }

    fn from_binary(buf: &mut BufferView) -> Option<char> {
        match buf.read_u8() {
            Ok(v) => Some(char::from(v)),
            _ => None,
        }
    }
}

impl Format for PrimitiveFormat<char> {
    type Iter = PrimitiveFormatIter<Self>;
    type Pusher = PrimitiveFormatPusher<Self>;

    fn into_iter(buf: Buffer, range: Range<usize>) -> PrimitiveFormatIter<Self> {
        PrimitiveFormatIter::new(buf, range)
    }

    fn into_pusher(buf: Buffer, range: Range<usize>) -> PrimitiveFormatPusher<Self> {
        PrimitiveFormatPusher::new(buf, range)
    }
}

impl Endian for PrimitiveFormat<(i32, i32)> {
    fn is_big_endian() -> bool {
        false
    }
}

impl DataSerializer for PrimitiveFormat<(i32, i32)> {
    type Item = (i32, i32);

    fn to_binary(buf: &mut BufferViewMut, ij: (i32, i32)) -> Option<()> {
        if Self::is_big_endian() {
            buf.write_i32::<BigEndian>(ij.0).ok();
            buf.write_i32::<BigEndian>(ij.1).ok()
        } else {
            buf.write_i32::<LittleEndian>(ij.0).ok();
            buf.write_i32::<LittleEndian>(ij.1).ok()
        }
    }

    fn from_binary(buf: &mut BufferView) -> Option<(i32, i32)> {
        let i = if Self::is_big_endian() {
            buf.read_i32::<BigEndian>()
        } else {
            buf.read_i32::<LittleEndian>()
        };

        let i = match i {
            Ok(v) => v,
            _ => return None,
        };

        let j = if Self::is_big_endian() {
            buf.read_i32::<BigEndian>()
        } else {
            buf.read_i32::<LittleEndian>()
        };

        let j = match j {
            Ok(v) => v,
            _ => return None,
        };

        Some((i, j))
    }
}

impl Format for PrimitiveFormat<(i32, i32)> {
    type Iter = PrimitiveFormatIter<Self>;
    type Pusher = PrimitiveFormatPusher<Self>;

    fn into_iter(buf: Buffer, range: Range<usize>) -> PrimitiveFormatIter<Self> {
        PrimitiveFormatIter::new(buf, range)
    }

    fn into_pusher(buf: Buffer, range: Range<usize>) -> PrimitiveFormatPusher<Self> {
        PrimitiveFormatPusher::new(buf, range)
    }
}

/// Interface for accessing the separator for string.
pub trait StringSeparator {
    const SEPARATOR: char;
}

/// Space as separator for string.
pub struct SpaceSeparator;

impl StringSeparator for SpaceSeparator {
    const SEPARATOR: char = ' ';
}

/// New line as separator for string.
pub struct NewLineSeparator;

impl StringSeparator for NewLineSeparator {
    const SEPARATOR: char = '\n';
}

/// String type format.
pub struct StringFormat<S: StringSeparator + Send>(PhantomData<S>);

impl<S> Endian for StringFormat<S> where S: StringSeparator + Send {}

impl<S> DataSerializer for StringFormat<S>
where
    S: StringSeparator + Send,
{
    type Item = String;

    fn to_binary(buf: &mut BufferViewMut, s: String) -> Option<()> {
        buf.write_all(s.as_bytes()).ok()
    }

    ///
    /// # Warning
    /// Deserializing string from byte buffer will read a string with all remaining bytes.
    fn from_binary(buf: &mut BufferView) -> Option<String> {
        let mut s = String::new();
        let result = buf.read_to_string(&mut s);

        match result {
            Ok(_) => Some(s),
            _ => None,
        }
    }
}

impl<S> Format for StringFormat<S>
where
    S: StringSeparator + Send,
{
    type Iter = IntoIter<String>;
    type Pusher = StringFormatPusher<Self>;

    fn into_iter(buf: Buffer, range: Range<usize>) -> IntoIter<String> {
        let mut buf_view: BufferView = Cursor::new(&buf[range]);
        let v = Self::reads(&mut buf_view);
        match v {
            Some(s) => {
                let mut ss: Vec<String> = s
                    .split(<S as StringSeparator>::SEPARATOR)
                    .map(|s| String::from(s))
                    .collect();
                // remove the last ""
                if let Some(last_s) = ss.last() {
                    if *last_s == String::new() {
                        ss.pop().unwrap();
                    }
                }
                ss.into_iter()
            }
            None => Vec::new().into_iter(),
        }
    }

    fn into_pusher(buf: Buffer, range: Range<usize>) -> StringFormatPusher<Self> {
        StringFormatPusher::new(buf, range)
    }
}

impl<S> StringSeparator for StringFormat<S>
where
    S: StringSeparator + Send,
{
    const SEPARATOR: char = S::SEPARATOR;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iterator_i16format() {
        let buf = vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06].into_boxed_slice();
        let range = 0..buf.len();
        let mut it = I16Format::into_iter(buf, range);

        // big endian
        assert_eq!(Some(0x0102), it.next());
        assert_eq!(Some(0x0304), it.next());
        assert_eq!(Some(0x0506), it.next());
        assert_eq!(None, it.next());
    }

    #[test]
    fn iterator_i32format() {
        let buf = vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08].into_boxed_slice();
        let range = 0..buf.len();
        let mut it = I32_FORMATTER.do_into_iter(buf, range);

        // big endian
        assert_eq!(Some(0x01020304), it.next());
        assert_eq!(Some(0x05060708), it.next());
        assert_eq!(None, it.next());
    }

    #[test]
    fn iterator_i32i32format() {
        let buf = vec![
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16,
            0x17, 0x18,
        ]
        .into_boxed_slice();
        let range = 0..buf.len();
        let mut it = I32I32Format::into_iter(buf, range);

        // ((little endian) lower mm addr, (little endian) higher mm addr)
        assert_eq!(Some((0x04030201, 0x08070605)), it.next());
        assert_eq!(Some((0x14131211, 0x18171615)), it.next());
        assert_eq!(None, it.next());
    }

    #[test]
    fn iterator_spacedstringformat() {
        let buf = "hello hurricane in rust "
            .to_owned()
            .into_boxed_str()
            .into_boxed_bytes();
        let range = 0..buf.len();
        let mut it = SpacedStringFormat::into_iter(buf, range);

        assert_eq!(Some(String::from("hello")), it.next());
        assert_eq!(Some(String::from("hurricane")), it.next());
        assert_eq!(Some(String::from("in")), it.next());
        assert_eq!(Some(String::from("rust")), it.next());
        assert_eq!(None, it.next());
    }

    #[test]
    fn iterator_linebylinestringformat() {
        let buf = "hello\nhurricane\nin\nrust\n"
            .to_owned()
            .into_boxed_str()
            .into_boxed_bytes();
        let range = 0..buf.len();
        let mut it = LINE_BY_LINE_STRING_FORMATTER.do_into_iter(buf, range);

        assert_eq!(Some(String::from("hello")), it.next());
        assert_eq!(Some(String::from("hurricane")), it.next());
        assert_eq!(Some(String::from("in")), it.next());
        assert_eq!(Some(String::from("rust")), it.next());
        assert_eq!(None, it.next());
    }

    #[test]
    fn pusher_i16format() {
        let buf: Buffer = Box::new([0 as u8; 6]);
        let range = 0..buf.len();
        let mut pu = I16Format::into_pusher(buf, range);

        // big endian
        assert_eq!(Some(()), pu.put(0x0110));
        assert_eq!(Some(()), pu.put(0x2332));
        assert_eq!(Some(()), pu.put(0x4554));
        assert_eq!(None, pu.put(0x0000));

        let buf = pu.into_inner();

        assert_eq!(buf[0], 0x01);
        assert_eq!(buf[1], 0x10);
        assert_eq!(buf[2], 0x23);
        assert_eq!(buf[3], 0x32);
        assert_eq!(buf[4], 0x45);
        assert_eq!(buf[5], 0x54);
    }

    #[test]
    fn pusher_i32format() {
        let buf: Buffer = Box::new([0 as u8; 8]);
        let range = 0..buf.len();
        let mut pu = I32_FORMATTER.do_into_pusher(buf, range);

        // big endian
        assert_eq!(Some(()), pu.put(0x01020304));
        assert_eq!(Some(()), pu.put(0x05060708));
        assert_eq!(None, pu.put(0x00000000));

        let buf = pu.into_inner();
        assert_eq!(&buf[..], [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    }

    #[test]
    fn pusher_i32i32format() {
        let buf: Buffer = Box::new([0 as u8; 16]);
        let range = 0..buf.len();
        let mut pu = I32I32Format::into_pusher(buf, range);

        // ((little endian) lower mm addr, (little endian) higher mm addr)
        assert_eq!(Some(()), pu.put((0x04030201, 0x08070605)));
        assert_eq!(Some(()), pu.put((0x14131211, 0x18171615)));
        assert_eq!(None, pu.put((0, 0)));

        let buf = pu.into_inner();
        assert_eq!(
            &buf[..],
            [
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16,
                0x17, 0x18
            ]
        );
    }

    #[test]
    fn pusher_spacedstringformat() {
        let buf: Buffer = Box::new([0 as u8; 24]);
        let range = 0..buf.len();
        let mut pu = SpacedStringFormat::into_pusher(buf, range);

        assert_eq!(Some(()), pu.put(String::from("hello")));
        assert_eq!(Some(()), pu.put(String::from("hurricane")));
        assert_eq!(Some(()), pu.put(String::from("in")));
        assert_eq!(Some(()), pu.put(String::from("rust")));
        assert_eq!(None, pu.put(String::new()));

        let buf = pu.into_inner();
        assert_eq!(&buf[..], "hello hurricane in rust ".as_bytes());
    }

    #[test]
    fn pusher_linebylinestringformat() {
        let buf: Buffer = Box::new([0 as u8; 24]);
        let range = 0..buf.len();
        let mut pu = LineByLineStringFormat::into_pusher(buf, range);

        assert_eq!(Some(()), pu.put(String::from("hello")));
        assert_eq!(Some(()), pu.put(String::from("hurricane")));
        assert_eq!(Some(()), pu.put(String::from("in")));
        assert_eq!(Some(()), pu.put(String::from("rust")));
        assert_eq!(None, pu.put(String::new()));

        let buf = pu.into_inner();
        assert_eq!(&buf[..], "hello\nhurricane\nin\nrust\n".as_bytes());
    }

    #[test]
    fn pusher_iterator_i32format() {
        let buf: Buffer = Box::new([0 as u8; 16]);
        let range = 0..buf.len();
        let mut pu = I32Format::into_pusher(buf, range);

        for i in -2..2 {
            assert_eq!(Some(()), pu.put(i));
        }
        assert_eq!(None, pu.put(0));

        let buf = pu.into_inner();
        let range = 0..buf.len();
        let mut it = I32Format::into_iter(buf, range);

        for i in -2..2 {
            assert_eq!(Some(i), it.next());
        }
        assert_eq!(None, it.next());
    }

    #[test]
    fn pusher_iterator_i64format() {
        let buf: Buffer = Box::new([0 as u8; 32]);
        let range = 0..buf.len();
        let mut pu = I64Format::into_pusher(buf, range);

        for i in -2..2 {
            assert_eq!(Some(()), pu.put(i));
        }
        assert_eq!(None, pu.put(0));

        let buf = pu.into_inner();
        let range = 0..buf.len();
        let mut it = I64Format::into_iter(buf, range);

        for i in -2..2 {
            assert_eq!(Some(i), it.next());
        }
        assert_eq!(None, it.next());
    }

    #[test]
    fn pusher_iterator_i16format() {
        let buf: Buffer = Box::new([0 as u8; 8]);
        let range = 0..buf.len();
        let mut pu = I16Format::into_pusher(buf, range);

        for i in -2..2 {
            assert_eq!(Some(()), pu.put(i));
        }
        assert_eq!(None, pu.put(0));

        let buf = pu.into_inner();
        let range = 0..buf.len();
        let mut it = I16Format::into_iter(buf, range);

        for i in -2..2 {
            assert_eq!(Some(i), it.next());
        }
        assert_eq!(None, it.next());
    }

    #[test]
    fn pusher_iterator_f32format() {
        let buf: Buffer = Box::new([0 as u8; 16]);
        let range = 0..buf.len();
        let mut pu = F32Format::into_pusher(buf, range);

        for f in -2..2 {
            assert_eq!(Some(()), pu.put(f as f32));
        }
        assert_eq!(None, pu.put(0 as f32));

        let (pos, buf) = pu.terminate();
        assert_eq!(pos, buf.len());
        let range = 0..buf.len();
        let mut it = F32Format::into_iter(buf, range);

        for f in -2..2 {
            assert_eq!(Some(f as f32), it.next());
        }
        assert_eq!(None, it.next());
    }

    #[test]
    fn pusher_iterator_f64format() {
        let buf: Buffer = Box::new([0 as u8; 32]);
        let range = 0..buf.len();
        let mut pu = F64Format::into_pusher(buf, range);

        for f in -2..2 {
            assert_eq!(Some(()), pu.put(f as f64));
        }
        assert_eq!(None, pu.put(0 as f64));

        let (pos, buf) = pu.terminate();
        assert_eq!(pos, buf.len());
        let range = 0..buf.len();
        let mut it = F64Format::into_iter(buf, range);

        for f in -2..2 {
            assert_eq!(Some(f as f64), it.next());
        }
        assert_eq!(None, it.next());
    }

    #[test]
    fn pusher_iterator_u8format() {
        let buf: Buffer = Box::new([0 as u8; 4]);
        let range = 0..buf.len();
        let mut pu = U8Format::into_pusher(buf, range);

        for b in 0..4 {
            assert_eq!(Some(()), pu.put(b as u8));
        }
        assert_eq!(None, pu.put(0 as u8));

        let buf = pu.into_inner();
        let range = 0..buf.len();
        let mut it = U8Format::into_iter(buf, range);

        for b in 0..4 {
            assert_eq!(Some(b as u8), it.next());
        }
        assert_eq!(None, it.next());
    }

    #[test]
    fn pusher_iterator_charformat() {
        let buf: Buffer = Box::new([0 as u8; 4]);
        let range = 0..buf.len();
        let mut pu = CharFormat::into_pusher(buf, range);

        for c in b'a'..b'e' {
            assert_eq!(Some(()), pu.put(c as char));
        }
        assert_eq!(None, pu.put(' ' as char));

        let buf = pu.into_inner();
        let range = 0..buf.len();
        let mut it = CharFormat::into_iter(buf, range);

        for c in b'a'..b'e' {
            assert_eq!(Some(c as char), it.next());
        }
        assert_eq!(None, it.next());
    }

    #[test]
    #[should_panic]
    fn pusher_iterator_charformat_invalid_char() {
        let buf: Buffer = Box::new([0 as u8; 4]);
        let range = 0..buf.len();
        let mut pu = CharFormat::into_pusher(buf, range);

        pu.put('笑');
    }

    #[test]
    fn pusher_iterator_i32i32format() {
        let buf: Buffer = Box::new([0 as u8; 32]);
        let range = 0..buf.len();
        let mut pu = I32I32Format::into_pusher(buf, range);

        for i in -2..2 {
            assert_eq!(Some(()), pu.put((i, i * 2)));
        }
        assert_eq!(None, pu.put((0, 0)));

        let buf = pu.into_inner();
        let range = 0..buf.len();
        let mut it = I32I32Format::into_iter(buf, range);

        for i in -2..2 {
            assert_eq!(Some((i, i * 2)), it.next());
        }
        assert_eq!(None, it.next());
    }

    #[test]
    fn pusher_iterator_spacedstringformat() {
        let buf: Buffer = Box::new([0 as u8; 24]);
        let range = 0..buf.len();
        let mut pu = SpacedStringFormat::into_pusher(buf, range);

        assert_eq!(Some(()), pu.put(String::from("hello")));
        assert_eq!(Some(()), pu.put(String::from("hurricane")));
        assert_eq!(Some(()), pu.put(String::from("in")));
        assert_eq!(Some(()), pu.put(String::from("rust")));
        assert_eq!(None, pu.put(String::new()));

        let buf = pu.into_inner();
        let range = 0..buf.len();
        let mut it = SpacedStringFormat::into_iter(buf, range);

        assert_eq!(Some(String::from("hello")), it.next());
        assert_eq!(Some(String::from("hurricane")), it.next());
        assert_eq!(Some(String::from("in")), it.next());
        assert_eq!(Some(String::from("rust")), it.next());
        assert_eq!(None, it.next());
    }

    #[test]
    fn pusher_iterator_linebylinestringformat() {
        let buf: Buffer = Box::new([0 as u8; 24]);
        let range = 0..buf.len();
        let mut pu = LineByLineStringFormat::into_pusher(buf, range);

        assert_eq!(Some(()), pu.put(String::from("hello")));
        assert_eq!(Some(()), pu.put(String::from("hurricane")));
        assert_eq!(Some(()), pu.put(String::from("in")));
        assert_eq!(Some(()), pu.put(String::from("rust")));
        assert_eq!(None, pu.put(String::from("exceedsbuffersize")));

        let buf = pu.into_inner();
        let range = 0..buf.len();
        let mut it = LineByLineStringFormat::into_iter(buf, range);

        assert_eq!(Some(String::from("hello")), it.next());
        assert_eq!(Some(String::from("hurricane")), it.next());
        assert_eq!(Some(String::from("in")), it.next());
        assert_eq!(Some(String::from("rust")), it.next());
        assert_eq!(None, it.next());
    }
}
