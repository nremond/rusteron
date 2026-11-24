//! One error in a distinct error log, as Agrona's `DistinctErrorLog` writes it
//! after a mark file's header: the same error observed again bumps its count
//! and last timestamp in place.

#![forbid(unsafe_code)]
// The crate allows these lints for its generated bindings; this module is checked.
#![warn(
    clippy::all,
    non_camel_case_types,
    non_snake_case,
    non_upper_case_globals,
    unsafe_op_in_unsafe_fn,
    unused_unsafe,
    unused_variables
)]

/// Offset of a record's length, written last.
pub const LENGTH_OFFSET: usize = 0;
/// Offset of how often the error was seen.
pub const OBSERVATION_COUNT_OFFSET: usize = 4;
/// Offset of when it was last seen, ms since the epoch.
pub const LAST_OBSERVATION_TIMESTAMP_OFFSET: usize = 8;
/// Offset of when it was first seen.
pub const FIRST_OBSERVATION_TIMESTAMP_OFFSET: usize = 16;
/// Offset of the error's text, ASCII.
pub const ENCODED_ERROR_OFFSET: usize = 24;
/// Records start on multiples of this.
pub const RECORD_ALIGNMENT: usize = 8;

/// A new record's bytes, its length field still 0: write them, then the
/// length (`record_length`) at [`LENGTH_OFFSET`], so a reader never sees a
/// record half written. Non-ASCII characters become `?`.
pub fn new_record(text: &str, timestamp_ms: i64) -> Vec<u8> {
    let mut record = vec![0; ENCODED_ERROR_OFFSET];
    record[OBSERVATION_COUNT_OFFSET..OBSERVATION_COUNT_OFFSET + 4].copy_from_slice(&1i32.to_le_bytes());
    record[LAST_OBSERVATION_TIMESTAMP_OFFSET..LAST_OBSERVATION_TIMESTAMP_OFFSET + 8]
        .copy_from_slice(&timestamp_ms.to_le_bytes());
    record[FIRST_OBSERVATION_TIMESTAMP_OFFSET..FIRST_OBSERVATION_TIMESTAMP_OFFSET + 8]
        .copy_from_slice(&timestamp_ms.to_le_bytes());
    record.extend(text.chars().map(|c| if c.is_ascii() { c as u8 } else { b'?' }));
    record
}

/// What goes at [`LENGTH_OFFSET`] once the record is in place.
pub fn record_length(record: &[u8]) -> i32 {
    record.len() as i32
}

/// The space a record takes before the next one starts.
pub fn aligned_length(record: &[u8]) -> usize {
    record.len().div_ceil(RECORD_ALIGNMENT) * RECORD_ALIGNMENT
}

/// One distinct error read back, as Agrona's `ErrorConsumer` is handed it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Observation {
    pub observation_count: i32,
    pub first_observation_timestamp_ms: i64,
    pub last_observation_timestamp_ms: i64,
    pub encoded_error: String,
}

/// Agrona's `ErrorLogReader.hasErrors`: a first record is in place.
pub fn has_errors(buffer: &[u8]) -> bool {
    read_i32(buffer, LENGTH_OFFSET).is_some_and(|length| length > 0)
}

/// Agrona's `ErrorLogReader.read`: each record last observed at or after
/// `since_timestamp_ms`, in the order the log holds them, up to the first
/// record not yet in place. Returns how many were handed to `consumer`.
pub fn read(buffer: &[u8], since_timestamp_ms: i64, mut consumer: impl FnMut(Observation)) -> usize {
    let mut entries = 0;
    let mut offset = 0;
    while offset < buffer.len() {
        let Some(length) = read_i32(buffer, offset + LENGTH_OFFSET).filter(|&length| length > 0) else {
            break;
        };
        let length = length as usize;
        let (Some(count), Some(last), Some(first)) = (
            read_i32(buffer, offset + OBSERVATION_COUNT_OFFSET),
            read_i64(buffer, offset + LAST_OBSERVATION_TIMESTAMP_OFFSET),
            read_i64(buffer, offset + FIRST_OBSERVATION_TIMESTAMP_OFFSET),
        ) else {
            break;
        };
        if last >= since_timestamp_ms {
            let text = buffer
                .get(offset + ENCODED_ERROR_OFFSET..(offset + length).min(buffer.len()))
                .unwrap_or_default();
            entries += 1;
            consumer(Observation {
                observation_count: count,
                first_observation_timestamp_ms: first,
                last_observation_timestamp_ms: last,
                encoded_error: String::from_utf8_lossy(text).into_owned(),
            });
        }
        offset += length.div_ceil(RECORD_ALIGNMENT) * RECORD_ALIGNMENT;
    }
    entries
}

fn read_i32(buffer: &[u8], at: usize) -> Option<i32> {
    Some(i32::from_le_bytes(buffer.get(at..at + 4)?.try_into().ok()?))
}

fn read_i64(buffer: &[u8], at: usize) -> Option<i64> {
    Some(i64::from_le_bytes(buffer.get(at..at + 8)?.try_into().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_read_back_in_order_from_the_given_time() {
        let mut buffer = vec![0u8; 256];
        let mut offset = 0;
        for (text, ts) in [("first", 10), ("second, longer", 20)] {
            let mut record = new_record(text, ts);
            let length = record_length(&record);
            record[LENGTH_OFFSET..4].copy_from_slice(&length.to_le_bytes());
            buffer[offset..offset + record.len()].copy_from_slice(&record);
            offset += aligned_length(&record);
        }
        assert!(has_errors(&buffer));
        let mut seen = Vec::new();
        assert_eq!(read(&buffer, 0, |o| seen.push(o)), 2);
        assert_eq!(seen[1].encoded_error, "second, longer");
        assert_eq!(
            (seen[0].observation_count, seen[0].last_observation_timestamp_ms),
            (1, 10)
        );
        assert_eq!(read(&buffer, 15, |_| {}), 1);
        assert!(!has_errors(&[0u8; 64]));
    }

    #[test]
    fn an_error_record_is_its_text_after_24_bytes() {
        let record = new_record("boom é", 5);
        assert_eq!(&record[24..], b"boom ?");
        assert_eq!(record_length(&record), 30);
        assert_eq!(aligned_length(&record), 32);
        assert_eq!(i32::from_le_bytes(record[4..8].try_into().unwrap()), 1);
        assert_eq!(
            i32::from_le_bytes(record[0..4].try_into().unwrap()),
            0,
            "the length is written last"
        );
    }
}
