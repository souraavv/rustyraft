use std::cell::RefCell;
use std::rc::Rc;

use rustyraft::raft::state_machine::StateMachine;

pub struct RecordingStateMachine {
    applied: Rc<
        RefCell<Vec<String>>
    >,
}

impl RecordingStateMachine {
    fn new(
        applied: Rc<
            RefCell<Vec<String>>
        >,
    ) -> Self {
        Self {
            applied,
        }
    }
}

impl StateMachine<String> for RecordingStateMachine {
    type Error = ();

    fn apply(
        &mut self,
        command: &String,
    ) -> Result<(), Self::Error> {
        self
            .applied
            .borrow_mut()
            .push(command.clone());

        Ok(())
    }
}

pub fn new_recording_state_machine() -> (
    RecordingStateMachine,
    Rc<RefCell<Vec<String>>>,
) {
    let applied: Rc<RefCell<Vec<String>>> =
        Rc::new(
            RefCell::new(
                Vec::new()
            )
        );

    let state_machine =
        RecordingStateMachine::new(
            Rc::clone(&applied)
        );

    (
        state_machine,
        applied,
    )
}

pub struct FailingStateMachine {
    applied: Vec<String>,
}

impl FailingStateMachine {
    fn new() -> Self {
        Self {
            applied: Vec::new(),
        }
    }
}

impl StateMachine<String> for FailingStateMachine {
    type Error = &'static str;

    fn apply(
        &mut self,
        command: &String,
    ) -> Result<(), Self::Error> {
        self.applied.push(
            command.clone()
        );

        if command == "B" {
            return Err(
                "failed to apply B"
            );
        }

        Ok(())
    }
}

pub fn new_failing_state_machine()
    -> FailingStateMachine
{
    FailingStateMachine::new()
}
