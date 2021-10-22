use std::io::Read;
use crate::IResult;
use crate::storage::File;
use crate::util::coding::encode_fixed_32;
use crate::util::crc32;
use crate::wal::{BLOCK_SIZE, HEADER_SIZE, MAX_RECORD_TYPE, RecordType};

pub struct Writer<F: File> {
    dest: F,
    /// Current offset int block.
    block_offset: usize,

    /// Crc32 values for all supported record types.There are pre-computed to reduce the overhead
    /// of computing the crc of the record type stored in the  head.
    crc: [u32; RecordType::KLastType as usize + 1 as usize],
}

impl<F: File> Writer<F> {
    pub fn new(f: F) -> Self {
        let n = RecordType::KLastType as usize;
        let mut crc = [0; RecordType::KLastType as usize + 1];

        for h in 1..=n {
            let v: [u8; 1] = [RecordType::from(h) as u8];
            crc[h as usize] = crc32::hash(&v);
        }

        Writer {
            dest: f,
            block_offset: 0,
            crc,
        }
    }

    /// Append a slice into the underlying log file.
    pub fn add_record(&mut self, s: &[u8]) -> IResult<()> {
        let mut left = s.len();
        let mut begin = true;
        // Fragment the record if necessary and emit it. Note that if slice
        // is empty, we still want to iterate once to emit a single
        // zero-length record.
        while {
            // Remaining capacity of the current block.
            let left_over = BLOCK_SIZE - self.block_offset;
            assert!(left_over >= 0, "[WAL Writer] current block space is zero");

            if left_over < HEADER_SIZE {
                // Switch to a new block (the left size if not enough for a record header).
                if left_over > 0 {
                    // Fill the trailer (literal below relies on HeaderSize being 7).
                    self.dest.write(&[0; 6][..left_over])?;
                }
                // Use a new block then reset offset.
                self.block_offset = 0;
            }

            // Invariant: we never leave less than HeaderSize bytes in a block.
            // Notice, if left capacity less than `HeaderSize` will use a new block,
            // so it must greater than  `HeaderSize`.
            assert!(BLOCK_SIZE - self.block_offset >= HEADER_SIZE,
                    "[WAL Writer] the left space of block {} is less than header size {}",
                    BLOCK_SIZE - self.block_offset,
                    HEADER_SIZE);

            // The available of current block(exclude header size).
            let avail = BLOCK_SIZE - self.block_offset - HEADER_SIZE;
            // Truncate if need to write record greater than `avail`.
            let fragment_length = if left < avail { left } else { avail };
            // If equals represent its can write to end.
            let end = left == fragment_length;
            let record_type =
                if begin && end {
                    RecordType::KFullType
                } else if begin {
                    RecordType::KFirstType
                } else if end {
                    RecordType::KLastType
                } else {
                    RecordType::KMiddleType
                };

            let start = s.len() - left;
            self.emit_physical_record(record_type, &s[start..start + fragment_length])?;
            left -= fragment_length;
            begin = false;

            // Notice: Why use it instead of while left > 0 ?
            // We needs to support add empty emppty.
            left > 0
        } {}
        Ok(())
    }


    pub fn emit_physical_record(&mut self, t: RecordType, data: &[u8]) -> IResult<()> {
        let length = data.len();
        assert!(length <= 0xffff,
                "[WAL Writer] the data length in a record must fit 2 bytes but got {}",
                length);

        // Format the header.
        let mut buf: [u8; HEADER_SIZE] = [0; HEADER_SIZE];
        buf[4] = (length & 0xff) as u8; // data length.
        buf[5] = (length >> 8) as u8;
        buf[6] = t as u8; // type.

        // Compute the crc of the record type and the payload.
        let mut crc = crc32::extend(self.crc[t as usize], data);
        crc = crc32::mask(crc);   // Adjust for storage.
        encode_fixed_32(&mut buf, crc);

        // Write the header and the payload.
        self.dest.write(&buf)?;
        self.dest.write(data)?;
        self.dest.flush();
        // Update block offset.
        self.block_offset += HEADER_SIZE + length;
        Ok(())
    }
}