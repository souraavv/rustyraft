//! Deterministic in-memory transport.
//!
//! This transport intentionally does not know about Raft nodes.
//! It only moves messages.
//! 
//! Messages are queued until the test or simulator explicitly
//! delivers them.

use std::collections::VecDeque;

use crate::raft::ServerId;

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

    pub fn deliver_to(
        &mut self,
        server_id: ServerId,
    ) -> Option<RaftMessage<C>> {
        let position = self
            .messages
            .iter()
            .position(|message| message.to == server_id)?;

        tracing::debug!(
            server_id = server_id.value(),
            "Delivering message to server"
        );
        self.messages.remove(position)
    }

    /// Drop one pending message for the given server.
    ///
    /// The message is removed from the transport and is never
    /// delivered to the destination.
    pub fn drop_to(
        &mut self,
        server_id: ServerId,
    ) -> bool {
        let position = self
            .messages
            .iter()
            .position(|message| {
                message.to == server_id
            });

        let position = match position {
            Some(position) => position,
            None => return false,
        };

        let message =
            self.messages.remove(position);

        if let Some(message) =
            message.as_ref()
        {
            tracing::debug!(
                from = message.from.value(),
                to = message.to.value(),
                "Dropped Raft message"
            );
        }

        true
    }

    /// Remove and return the message at the given queue position.
    ///
    /// This is used by deterministic tests to intentionally
    /// reorder messages.
    pub fn deliver_at(
        &mut self,
        position: usize,
    ) -> Option<RaftMessage<C>> {
        let message =
            self.messages.get(position)?;

        tracing::debug!(
            from = message.from.value(),
            to = message.to.value(),
            position,
            "Delivering Raft message by queue position"
        );

        self.messages.remove(position)
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