//! Deterministic in-memory transport.
//!
//! This transport intentionally does not know about Raft nodes.
//! It only moves messages.
//! 
//! Messages are queued until the test or simulator explicitly
//! delivers them.

use std::collections::VecDeque;

use super::RaftMessage;

#[derive(Debug)]
pub struct InMemoryTransport<C> {
    messages: VecDeque<RaftMessage<C>>,
}

impl<C> InMemoryTransport<C> {
    pub fn new() -> Self {
        Self {
            messages: VecDeque::new(),
        }
    }

    pub fn send(&mut self, message: RaftMessage<C>) {
        tracing::debug!(
            from = message.from.value(),
            to = message.to.value(),
            "Queued Raft message"
        );
        self.messages.push_back(message);
    }

    // returns the next message without removing this
    pub fn peek(&self) -> Option<&RaftMessage<C>> {
        self.messages.front()
    }

    // remove and return the next pending message
    pub fn deliver_next(&mut self) -> Option<RaftMessage<C>> {
        let message = self.messages.pop_front();

        if let Some(ref message) = message {
            tracing::debug!(
                from = message.from.value(),
                to = message.to.value(),
                "Delivered Raft message"
            );
        }
        
        message
    }

    pub fn has_pending(&self) -> bool {
        !self.messages.is_empty()
    }

    pub fn pending_count(&self) -> usize {
        self.messages.len()
    }
}

impl<C> Default for InMemoryTransport<C> {
    fn default() -> Self {
        Self::new()
    }
}