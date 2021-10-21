use std::io::SeekFrom;
use std::sync::Arc;
use crate::IResult;
use crate::storage::File;
use crate::util::coding::decode_fixed_32;
use crate::wal::{BLOCK_SIZE, HEADER_SIZE, RecordType};
use crate::util::crc32;

enum ReportError {
    EOF,
    BadRecord,
}

#[derive(Clone)]
struct Record {
    t: RecordType,
    data: Vec<u8>,
}

pub trait Reporter {
    /// Some corruption was detected. `size` is the approximate number
    /// of bytes dropped due to the corruption.
    fn corruption(&self, bytes: u64, reason: &str) -> IResult<()>;
}

pub struct Reader<F: File> {
    file: F,
    reporter: Option<Box<dyn Reporter>>,
    // whether check sum for the record or not.
    checksum: bool,
    buffer: Vec<u8>,
    eof: bool,
    last_record_offset: u64,
    end_of_buffer_offset: u64,
    initial_offset: u64,
    resyncing: bool,
}

impl<F: File> Reader<F> {
    pub fn new(
        f: F,
        reporter: Option<Box<dyn Reporter>>,
        checksum: bool,
        initial_offset: u64,
    ) -> Self {
        Reader {
            file: f,
            reporter,
            checksum,
            buffer: vec![0; BLOCK_SIZE],
            eof: false,
            last_record_offset: 0,
            end_of_buffer_offset: 0,
            initial_offset,
            resyncing: initial_offset > 0,
        }
    }

    pub fn read_record(&mut self, buf: &mut Vec<u8>) -> bool {
        if self.last_record_offset < self.initial_offset
            && !self.skip_to_initial_block() {
            return false;
        }
        let mut in_fragmented_record = false;
        // Record offset of the logical record that we're reading
        // 0 is a dummy value to make compilers happy.
        let mut prospective_record_offset = 0;

        loop {
            match self.read_physical_record() {
                Ok(mut record) => {
                    if self.resyncing {
                        match record.t {
                            RecordType::KMiddleType => continue,
                            RecordType::KLastType => {
                                self.resyncing = false;
                                continue;
                            }
                            _ => self.resyncing = false,
                        }
                    }

                    let physical_record_offset = self.end_of_buffer_offset - self.buffer.len() as u64 - HEADER_SIZE as u64 - record.data.len() as u64;

                    match record.t {
                        RecordType::KFullType => {
                            if in_fragmented_record {
                                self.report_drop(buf.len() as u64, "partial without end(1).");
                            }
                            prospective_record_offset = physical_record_offset;
                            buf.clear();
                            buf.append(&mut record.data);
                            self.last_record_offset = prospective_record_offset;
                            return true;
                        }
                        RecordType::KFirstType => {
                            if in_fragmented_record {
                                self.report_drop(buf.len() as u64, "partial without end(2).");
                            }
                            prospective_record_offset = physical_record_offset;
                            buf.clear();
                            buf.append(&mut record.data);
                            in_fragmented_record = true;
                            break;
                        }
                        RecordType::KMiddleType => {
                            if !in_fragmented_record {
                                self.report_drop(
                                    record.data.len() as u64,
                                    format!(
                                        "missing start of fragmented record({:?})",
                                        RecordType::KMiddleType
                                    )
                                        .as_str(),
                                );
                                // continue reading until find a new first or full record
                            } else {
                                buf.append(&mut record.data);
                            }
                            break;
                        }
                        RecordType::KLastType => {
                            if !in_fragmented_record {
                                self.report_drop(
                                    record.data.len() as u64,
                                    format!(
                                        "missing start of fragmented record({:?})",
                                        RecordType::KLastType
                                    )
                                        .as_str(),
                                );
                                // continue reading until find a new first or full record
                            } else {
                                buf.extend(record.data);
                                // notice that we update the last_record_offset after we get the Last part but not the First
                                self.last_record_offset = prospective_record_offset;
                                return true;
                            }
                            break;
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    match e {
                        ReportError::EOF => {
                            if in_fragmented_record {
                                buf.clear();
                            }
                            return false;
                        }
                        ReportError::BadRecord => {
                            if in_fragmented_record {
                                self.report_drop(buf.len() as u64, "error in middle of record.");
                                in_fragmented_record = false;
                                buf.clear();
                            }
                            break;
                        }
                    }
                }
            }
        }
        true
    }

    /// Return type, or one of the preceding special values.
    fn read_physical_record(&mut self) -> Result<Record, ReportError> {
        loop {
            if self.buffer.len() < HEADER_SIZE {
                self.buffer.clear();
                if !self.eof {
                    // Try to read a block into buf.
                    match self.file.read(&mut self.buffer) {
                        Ok(n) => {
                            self.end_of_buffer_offset += n as u64;
                            if n < BLOCK_SIZE {
                                self.eof = true
                            }
                        }
                        Err(e) => {
                            self.report_drop(BLOCK_SIZE as u64, &e.to_string());
                            self.eof = true;
                            return Err(ReportError::EOF);
                        }
                    }
                    continue;
                } else {
                    return Err(ReportError::EOF);
                }
            }

            // Parse the header.
            let header = &self.buffer[0..HEADER_SIZE];
            let a = header[4] & 0xff;
            let b = header[5] & 0xff;
            let record_type = header[6];
            let length = a | (b << 8);
            if HEADER_SIZE + length as usize > self.buffer.len() {
                let drop_size = self.buffer.len();
                self.buffer.clear();
                if !self.eof {
                    self.report_drop(drop_size as u64, "bad record length.");
                    return Err(ReportError::BadRecord);
                }
                return Err(ReportError::EOF);
            }

            if record_type == 0 && length == 0 {
                self.buffer.clear();
                return Err(ReportError::BadRecord);
            }

            // Check crc
            if self.checksum {
                let expected_crc = crc32::unmask(decode_fixed_32(header));
                let actual_crc = crc32::hash(&self.buffer[HEADER_SIZE - 1..HEADER_SIZE + length as usize]);
                if actual_crc != expected_crc {
                    let drop_size = self.buffer.len();
                    self.buffer.clear();
                    self.report_drop(drop_size as u64, "checksum mismatch.");
                    return Err(ReportError::BadRecord);
                }
            }

            let mut data = self.buffer.drain(0..HEADER_SIZE + length as usize).collect::<Vec<u8>>();

            // Skip physical record that started before `initial_offset`.
            if (self.end_of_buffer_offset - self.buffer.len() as u64 - HEADER_SIZE as u64 - length as u64) < self.initial_offset {
                return Err(ReportError::BadRecord);
            }

            data.drain(0..HEADER_SIZE);

            return Ok(Record {
                t: RecordType::from(record_type as usize),
                data,
            });
        }
    }

    pub fn last_record_offset(&self) -> u64 {
        self.last_record_offset
    }

    /// Skips all blocks that are completely before `initial_offset`.
    ///
    /// Return true on success. Handles reporting.
    fn skip_to_initial_block(&mut self) -> bool {
        let offset_in_block = self.initial_offset % BLOCK_SIZE as u64;
        let mut block_start_location = self.initial_offset - offset_in_block;

        // Don't search a block if we'd be in the trailer.
        if offset_in_block > (BLOCK_SIZE - 6) as u64 {
            block_start_location += BLOCK_SIZE as u64;
        }

        self.end_of_buffer_offset = block_start_location;

        // Skip to start of first block that can contain the initial record.
        if block_start_location > 0 {
            if let Err(e) = self.file.seek(SeekFrom::Start(block_start_location)) {
                self.report_drop(block_start_location, &e.to_string());
                return false;
            }
        }
        true
    }

    /// Reports dropped bytes to the reporter.
    fn report_drop(&self, bytes: u64, reason: &str) {
        if let Some(reporter) = self.reporter.as_ref() {
            if self.end_of_buffer_offset - self.buffer.len() as u64 - bytes >= self.initial_offset {
                reporter.corruption(bytes, reason);
            }
        }
    }
}