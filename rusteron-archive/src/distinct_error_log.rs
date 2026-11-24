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

#[cfg(test)]
mod tests {
    use super::*;

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
