//! Frames from an image, reassembled into messages: what Java's
//! `BoundedLogAdapter` does with a `BufferBuilder` for the cluster's log.

use crate::position::{BEGIN_FRAGMENT, END_FRAGMENT, FRAME_ALIGNMENT, HEADER_LENGTH, UNFRAGMENTED, align};

/// What a fragment handler tells the image's controlled poll.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PollAction {
    /// The fragment is consumed; poll on.
    Continue,
    /// The fragment is consumed; stop this poll.
    Break,
    /// The fragment is not consumed: the next poll delivers it again.
    Abort,
}

/// A frame as an image delivers it.
#[derive(Clone, Copy, Debug)]
pub struct Fragment<'a> {
    pub payload: &'a [u8],
    /// The frame's flags: [`UNFRAGMENTED`], or begin and end of a fragmented message.
    pub flags: u8,
    /// Where the frame starts in its term.
    pub term_offset: i32,
    /// The stream position just after the frame.
    pub position: i64,
}

/// Reassembles fragmented messages. A frame that does not follow the one
/// before it in its term drops the message under way, as Java does.
#[derive(Debug, Default)]
pub struct Reassembler {
    buffer: Vec<u8>,
    next_term_offset: Option<i32>,
}

impl Reassembler {
    /// Hands each whole message to `on_message` with the position just after
    /// its last frame. On [`PollAction::Abort`] the last fragment is taken back,
    /// so the next delivery of it completes the message again.
    pub fn on_fragment(
        &mut self,
        fragment: Fragment<'_>,
        on_message: impl FnOnce(&[u8], i64) -> PollAction,
    ) -> PollAction {
        let flags = fragment.flags;
        if flags & UNFRAGMENTED == UNFRAGMENTED {
            return on_message(fragment.payload, fragment.position);
        }
        if flags & BEGIN_FRAGMENT == BEGIN_FRAGMENT {
            self.buffer.clear();
            self.buffer.extend_from_slice(fragment.payload);
            self.next_term_offset = Some(Self::next_frame(fragment));
            return PollAction::Continue;
        }
        if self.next_term_offset != Some(fragment.term_offset) {
            self.reset();
            return PollAction::Continue;
        }
        let limit = self.buffer.len();
        self.buffer.extend_from_slice(fragment.payload);
        if flags & END_FRAGMENT != END_FRAGMENT {
            self.next_term_offset = Some(Self::next_frame(fragment));
            return PollAction::Continue;
        }
        let message = std::mem::take(&mut self.buffer);
        let action = on_message(&message, fragment.position);
        self.buffer = message;
        if action == PollAction::Abort {
            self.buffer.truncate(limit);
        } else {
            self.reset();
        }
        action
    }

    /// Drops a message under way: the image it came from is gone.
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.next_term_offset = None;
    }

    fn next_frame(fragment: Fragment<'_>) -> i32 {
        let end = fragment.term_offset as i64 + HEADER_LENGTH as i64 + fragment.payload.len() as i64;
        align(end, FRAME_ALIGNMENT as i64) as i32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(payload: &[u8], flags: u8, term_offset: i32) -> Fragment<'_> {
        let position = term_offset as i64 + crate::position::aligned_frame_length(payload.len() as i32) as i64;
        Fragment {
            payload,
            flags,
            term_offset,
            position,
        }
    }

    #[test]
    fn an_unfragmented_message_passes_straight_through() {
        let mut r = Reassembler::default();
        let mut seen = None;
        let action = r.on_fragment(frame(b"abc", UNFRAGMENTED, 64), |m, p| {
            seen = Some((m.to_vec(), p));
            PollAction::Continue
        });
        assert_eq!(action, PollAction::Continue);
        assert_eq!(seen, Some((b"abc".to_vec(), 128)));
    }

    #[test]
    fn fragments_join_at_the_position_of_the_last() {
        let mut r = Reassembler::default();
        let a = [1u8; 40];
        let b = [2u8; 40];
        let c = [3u8; 7];
        assert_eq!(
            r.on_fragment(frame(&a, BEGIN_FRAGMENT, 0), |_, _| unreachable!()),
            PollAction::Continue
        );
        assert_eq!(
            r.on_fragment(frame(&b, 0, 96), |_, _| unreachable!()),
            PollAction::Continue
        );
        let mut seen = None;
        r.on_fragment(frame(&c, END_FRAGMENT, 192), |m, p| {
            seen = Some((m.len(), p));
            PollAction::Break
        });
        assert_eq!(seen, Some((87, 192 + 64)));
    }

    #[test]
    fn a_gap_drops_the_message() {
        let mut r = Reassembler::default();
        r.on_fragment(frame(&[1; 40], BEGIN_FRAGMENT, 0), |_, _| unreachable!());
        assert_eq!(
            r.on_fragment(frame(&[2; 4], END_FRAGMENT, 128), |_, _| unreachable!()),
            PollAction::Continue
        );
    }

    #[test]
    fn an_abort_takes_back_the_last_fragment_only() {
        let mut r = Reassembler::default();
        r.on_fragment(frame(&[1; 40], BEGIN_FRAGMENT, 0), |_, _| unreachable!());
        let end = [2u8; 4];
        assert_eq!(
            r.on_fragment(frame(&end, END_FRAGMENT, 96), |_, _| PollAction::Abort),
            PollAction::Abort
        );
        let mut length = 0;
        r.on_fragment(frame(&end, END_FRAGMENT, 96), |m, _| {
            length = m.len();
            PollAction::Continue
        });
        assert_eq!(length, 44);
    }
}
