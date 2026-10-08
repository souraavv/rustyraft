//! Deterministic in-memory transport.
//!
//! This transport intentionally does not know about Raft nodes.
//! It only moves messages.
//!
//! Messages are queued until the test or simulator explicitly
//! delivers them.
//!
//! The transport can simulate both directional and bidirectional
//! network partitions.

use std::collections::{
    HashSet,
    VecDeque,
};

use crate::raft::{ServerId, transport::{RaftMessage, Transport, tcp::TcpTransportError}};

#[derive(Debug)]
pub struct InMemoryTransport<C> {
    messages: VecDeque<RaftMessage<C>>,

    // Stores blocked communication links as
    // (from, to) pairs.
    //
    // (A, B) means A -> B is blocked.
    // (B, A) means B -> A is blocked.
    blocked_links: HashSet<(ServerId, ServerId)>,
}

impl<C> InMemoryTransport<C> {
    pub fn new() -> Self {
        Self {
            messages: VecDeque::new(),
            blocked_links: HashSet::new(),
        }
    }

    pub fn send(
        &mut self,
        message: RaftMessage<C>,
    ) {
        if self.is_partitioned(
            message.from,
            message.to,
        ) {
            tracing::debug!(
                from = message.from.value(),
                to = message.to.value(),
                "Dropped Raft message because transport link is partitioned"
            );

            return;
        }

        tracing::debug!(
            from = message.from.value(),
            to = message.to.value(),
            "Queued Raft message"
        );

        self.messages.push_back(message);
    }

    // returns the next message without removing this
    pub fn peek(
        &self,
    ) -> Option<&RaftMessage<C>> {
        self.messages.front()
    }

    // remove and return the next pending message
    pub fn deliver_next(
        &mut self,
    ) -> Option<RaftMessage<C>> {
        let message =
            self.messages.pop_front();

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
            .position(|message| {
                message.to == server_id
            })?;

        tracing::debug!(
            server_id = server_id.value(),
            "Delivering message to server"
        );

        self.messages.remove(position)
    }

    /// Remove and return the first pending message that matches the
    /// supplied predicate
    /// 
    /// This is used by deterministic tests to deliver a specific message
    /// while leaving unrelated messages in the transport queue.
    pub fn deliver_matching<F>(
        &mut self, 
        predicate: F,
    ) -> Option<RaftMessage<C>>
    where 
        F: Fn(&RaftMessage<C>) -> bool,
        C: Clone,
    {
        let position = self
            .messages
            .iter()
            .position(predicate)?;

        let message =
            self.messages.remove(position);

        if let Some(ref message) = message {
            tracing::debug!(
                from = message.from.value(),
                to = message.to.value(),
                "Delivering matching Raft message"
            );
        }

        message
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

    /// Partition communication from one server to another.
    ///
    /// This is directional.
    ///
    /// partition(A, B) blocks A -> B but does not block
    /// B -> A.
    pub fn partition(
        &mut self,
        from: ServerId,
        to: ServerId,
    ) {
        self.blocked_links.insert(
            (from, to),
        );

        tracing::debug!(
            from = from.value(),
            to = to.value(),
            "Partitioned transport link"
        );
    }

    /// Heal communication from one server to another.
    ///
    /// This is directional.
    ///
    /// heal(A, B) allows A -> B again but does not
    /// change the B -> A link.
    pub fn heal(
        &mut self,
        from: ServerId,
        to: ServerId,
    ) {
        self.blocked_links.remove(
            &(from, to),
        );

        tracing::debug!(
            from = from.value(),
            to = to.value(),
            "Healed transport link"
        );
    }

    pub fn duplicate_matching<F>(
        &mut self,
        predicate: F,
    ) -> bool
    where
        F: Fn(&RaftMessage<C>) -> bool,
        C: Clone,
    {
        let message = self
            .messages
            .iter()
            .find(|message| predicate(message))
            .cloned();

        match message {
            Some(message) => {
                self.messages.push_back(message);
                true
            }
            None => false,
        }
    }

    /// Partition communication in both directions between
    /// two servers.
    ///
    /// This is equivalent to:
    ///
    /// partition(A, B)
    /// partition(B, A)
    pub fn partition_bidirectional(
        &mut self,
        first: ServerId,
        second: ServerId,
    ) {
        self.partition(
            first,
            second,
        );

        self.partition(
            second,
            first,
        );
    }

    /// Heal communication in both directions between
    /// two servers.
    ///
    /// This is equivalent to:
    ///
    /// heal(A, B)
    /// heal(B, A)
    pub fn heal_bidirectional(
        &mut self,
        first: ServerId,
        second: ServerId,
    ) {
        self.heal(
            first,
            second,
        );

        self.heal(
            second,
            first,
        );
    }

    /// Returns true when communication from `from` to `to`
    /// is currently blocked.
    pub fn is_partitioned(
        &self,
        from: ServerId,
        to: ServerId,
    ) -> bool {
        self.blocked_links.contains(
            &(from, to),
        )
    }

    pub fn has_pending(
        &self,
    ) -> bool {
        !self.messages.is_empty()
    }

    pub fn pending_count(
        &self,
    ) -> usize {
        self.messages.len()
    }
}

impl<C> Default for InMemoryTransport<C> {
    fn default() -> Self {
        Self::new()
    }
}

/// Implement the transport interface for the InMemoryTransport
impl<C> Transport<C> for InMemoryTransport<C> {
    
    type Error = std::convert::Infallible;
    
    async fn send(
        &mut self,
        message: RaftMessage<C>,
    ) -> Result<(), TcpTransportError>
    {
        InMemoryTransport::send(
            self,
            message,
        );

        Ok(())
    }

    async fn receive(
        &mut self,
    ) -> Option<RaftMessage<C>> {
        self.deliver_next()
    }
}