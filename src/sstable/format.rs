use crate::error::Error;
use crate::IResult;
use crate::opt::{CompressionType, ReadOptions};
use crate::storage::File;
use crate::util::coding::{decode_fixed_32, decode_fixed_64, put_fixed_32, put_fixed_64, VarintU64};
use crate::util::crc32::{hash, unmask};

/// Maximum encoding length of a `BlockHandle`.
const K_MAX_ENCODED_LENGTH: u64 = 10 + 10;

/// Encoded length of a `Footer`. Note that the serialization of a
/// `Footer` will always occupy exactly this many bytes. It consists
/// of two block handles and a magic number.
pub const K_ENCODED_LENGTH: u64 = 2 * K_MAX_ENCODED_LENGTH + 8;

/// Magic number.
const K_TABLE_MAGIC_NUMBER: u64 = 0xdb4775248b80fb57;

/// 1-byte type  +   32-bit crc.
pub const K_BLOCK_TRAILER_SIZE: usize = 5;

/// BlockHandle is a pointer to the extent of a file that stores a data
/// block or a meta block.
pub struct BlockHandle {
    offset: u64,
    size: u64,
}

impl BlockHandle {
    pub fn new(offset: u64, size: u64) -> Self {
        BlockHandle {
            offset,
            size,
        }
    }

    /// The offset of the block in the file.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// The size of the stored block.lk
    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn set_offset(&mut self, offset: u64) {
        self.offset = offset;
    }

    pub fn set_size(&mut self, size: u64) {
        self.size = size;
    }

    #[inline]
    pub fn encoded(&self) -> Vec<u8> {
        let mut v = vec![];
        self.encode_to(&mut v);
        v
    }

    pub fn encode_to(&self, dst: &mut Vec<u8>) {
        // Sanity check that all fields have been set.
        assert_eq!(self.offset,
                   0,
                   "[BlockHandle] offset must be set, but got 0");
        assert_eq!(self.size,
                   0,
                   "[BlockHandle] size must be set, but got 0");
        VarintU64::put_varint(dst, self.offset);
        VarintU64::put_varint(dst, self.size);
    }

    pub fn decode_from(src: &[u8]) -> IResult<(Self, usize)> {
        if let Some((offset, n)) = VarintU64::read(src) {
            if let Some((size, m)) = VarintU64::read(&src[n..]) {
                Ok((Self::new(offset, size), m + n))
            } else {
                Err(Error::Corruption("bad block handle."))
            }
        } else {
            Err(Error::Corruption("bad block handle."))
        }
    }
}

/// Footer encapsulates the fixed information stored at the tail
/// end of every table file.
pub struct Footer {
    pub metaindex_handle: BlockHandle,
    pub index_handle: BlockHandle,
}

impl Footer {
    pub fn new(metaindex_handle: BlockHandle, index_handle: BlockHandle) -> Self {
        Footer {
            metaindex_handle,
            index_handle,
        }
    }

    /// Encodeds footer and returns the encoded bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut v = vec![];
        self.metaindex_handle.encode_to(&mut v);
        self.index_handle.encode_to(&mut v);
        dst.resize(2 * K_MAX_ENCODED_LENGTH as usize, 0);
        put_fixed_64(&mut v, K_TABLE_MAGIC_NUMBER);
        assert_eq!(
            v.len() as u64,
            K_ENCODED_LENGTH,
            "[Footer] the length of encoded footer is {}, expect {}",
            v.len(),
            K_ENCODED_LENGTH,
        );
        v
    }

    pub fn decode_from(src: &[u8]) -> IResult<Self> {
        let magic = decode_fixed_64(&src[K_ENCODED_LENGTH as usize - 8..]);
        if magic != K_TABLE_MAGIC_NUMBER {
            return Err(Error::Corruption("not an sstable (bad magic number.)"));
        }
        let (metaindex_handle, n) = BlockHandle::decode_from(src)?;
        let (index_handle, _) = BlockHandle::decode_from(&src[n..])?;
        Ok(Footer::new(metaindex_handle, index_handle))
    }
}

/// Read the block identified from `file` according to the given `handle`.
/// If the read data dose not match the checksum, return a error marked as `Error::Corruption`.
pub fn read_block<F: File>(f: F, verify_checksums: bool, handle: &BlockHandle) -> IResult<Vec<u8>> {
    // Read the block contents as well as the type/crc footer.
    // See `TableBuilder` for the code that built this structure.
    let n = handle.size as usize;
    let mut buf = vec![0; n + K_BLOCK_TRAILER_SIZE];
    f.read_exact_at(buf.as_mut_slice(), handle.offset)?;

    // Check crc.
    if verify_checksums {
        // Obtain the last four bytes in buf.
        let crc = unmask(decode_fixed_32(&buf[n + 1..]));
        let actual = hash(&buf[..=n]);
        if crc != actual {
            return Err(Error::Corruption("block checksum mismatch."));
        }
    }

    // Determine whether deCompression is required based on type.
    let data = match CompressionType::from(buf[n]) {
        CompressionType::KNoCompression => {
            buf.truncate(buf.len() - K_BLOCK_TRAILER_SIZE);
            buf
        }
        CompressionType::KSnappyCompression => {
            let mut decompressed = vec![];
            match snap::raw::decompress_len(&buf[..n]) {
                Ok(len) => {
                    decompressed.resize(len, 0u8);
                }
                Err(e) => {
                    return Err(Error::CompressedFailed(e));
                }
            }
            let mut dec = snap::raw::Decoder::new();
            if let Err(e) = dec.decompress(&buf[..n], decompressed.as_mut_slice()) {
                return Err(Error::CompressedFailed(e));
            }
            decompressed
        }
        CompressionType::UnKnown => {
            return Err(Error::Corruption("bad block compression type."));
        }
    };

    Ok(data)
}