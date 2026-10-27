//! Aeron's log arithmetic: how messages become frames in a term, and frames
//! positions. Positions in the cluster's log are stream positions: a message's
//! is the position just after its last frame.

/// Every frame starts on a multiple of this.
pub const FRAME_ALIGNMENT: i32 = 32;
/// The data frame header every frame starts with.
pub const HEADER_LENGTH: i32 = 32;

/// A frame that is the whole message.
pub const UNFRAGMENTED: u8 = 0b1100_0000;
/// The first frame of a fragmented message.
pub const BEGIN_FRAGMENT: u8 = 0b1000_0000;
/// The last frame of a fragmented message.
pub const END_FRAGMENT: u8 = 0b0100_0000;

/// `value` rounded up to a multiple of `alignment`, a power of two.
pub const fn align(value: i64, alignment: i64) -> i64 {
    (value + (alignment - 1)) & !(alignment - 1)
}

/// The bits a term count is shifted by to make a position: log2 of the term length.
pub const fn position_bits_to_shift(term_length: i32) -> u32 {
    term_length.trailing_zeros()
}

/// The stream position of `term_offset` in term `term_id`.
pub const fn compute_position(
    term_id: i32,
    term_offset: i32,
    position_bits_to_shift: u32,
    initial_term_id: i32,
) -> i64 {
    let term_count = term_id.wrapping_sub(initial_term_id) as i64;
    (term_count << position_bits_to_shift) + term_offset as i64
}

/// The most payload one frame carries on a channel of this MTU.
pub const fn max_payload_length(mtu: i32) -> i32 {
    mtu - HEADER_LENGTH
}

/// The largest message a publication with terms this long takes: an eighth
/// of a term, at most 16 MiB.
pub const fn max_message_length(term_length: i32) -> i32 {
    let eighth = term_length / 8;
    if eighth < 16 * 1024 * 1024 {
        eighth
    } else {
        16 * 1024 * 1024
    }
}

/// How much of a term a frame of `payload_length` takes.
pub const fn aligned_frame_length(payload_length: i32) -> i32 {
    align((payload_length + HEADER_LENGTH) as i64, FRAME_ALIGNMENT as i64) as i32
}

/// The frames a message of `length` is split into at `max_payload`: the
/// payload length of each, in order.
pub fn fragment_lengths(length: usize, max_payload: usize) -> Vec<usize> {
    if length <= max_payload {
        return vec![length];
    }
    let mut lengths = Vec::with_capacity(length.div_ceil(max_payload));
    let mut remaining = length;
    while remaining > 0 {
        let next = remaining.min(max_payload);
        lengths.push(next);
        remaining -= next;
    }
    lengths
}

/// How much of a term a message of `length` takes, all its frames together.
pub fn message_term_length(length: usize, max_payload: usize) -> i32 {
    fragment_lengths(length, max_payload)
        .into_iter()
        .map(|l| aligned_frame_length(l as i32))
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_are_aligned_with_their_header() {
        assert_eq!(aligned_frame_length(0), 32);
        assert_eq!(aligned_frame_length(1), 64);
        assert_eq!(aligned_frame_length(32), 64);
        assert_eq!(aligned_frame_length(33), 96);
        // A session message header alone: 8 bytes of SBE header and a block of 24.
        assert_eq!(aligned_frame_length(32), 64);
    }

    #[test]
    fn positions_count_whole_terms() {
        let bits = position_bits_to_shift(64 * 1024);
        assert_eq!(bits, 16);
        assert_eq!(compute_position(7, 0, bits, 7), 0);
        assert_eq!(compute_position(7, 4_096, bits, 7), 4_096);
        assert_eq!(compute_position(9, 32, bits, 7), 2 * 65_536 + 32);
        // Term ids wrap; the count of terms does not.
        assert_eq!(compute_position(i32::MIN, 0, bits, i32::MAX), 65_536);
    }

    #[test]
    fn messages_fragment_at_the_max_payload() {
        assert_eq!(fragment_lengths(0, 1_376), vec![0]);
        assert_eq!(fragment_lengths(1_376, 1_376), vec![1_376]);
        assert_eq!(fragment_lengths(1_377, 1_376), vec![1_376, 1]);
        assert_eq!(fragment_lengths(4_000, 1_376), vec![1_376, 1_376, 1_248]);
        assert_eq!(message_term_length(4_000, 1_376), 1_408 + 1_408 + 1_280);
    }

    #[test]
    fn max_message_is_an_eighth_of_a_term() {
        assert_eq!(max_message_length(64 * 1024), 8 * 1024);
        assert_eq!(max_message_length(1 << 30), 16 * 1024 * 1024);
        assert_eq!(max_payload_length(1_408), 1_376);
    }
}
