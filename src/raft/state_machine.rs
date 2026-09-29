//! Raft State machine
//! 
//! The state machine receives commited Raft log commands
//! 
//! Raft doesn't defines what the application state machine looks like 
//! It only guarantees that commited commands are applied in log order
//! 
//! The state machine is deliberately kept separate from Raft Log
//! 

/// State machine 
/// 
/// A state machine receives commands only after the coresponding Raft log
/// entries has been commited
/// 
/// The command is borrowed because the command remain part of Raft log after
/// it is applied
pub trait StateMachine<C> {
    // every implementation of this trait must choose what its error type is
    type Error;
    /// Applies a commited command to the state machine
    fn apply(
        &mut self, 
        command: &C
    ) -> Result<(), Self::Error>;
}

/// State machine used when a Raft node does not need to apply
/// commands yet.

#[derive(Debug, Default)]
pub struct NoopStateMachine; 

impl<C> StateMachine<C> for NoopStateMachine {
    type Error = ();

    fn apply(
        &mut self, 
        command: &C
    ) -> Result<(), Self::Error>
    {
        Ok(())
    }
}

