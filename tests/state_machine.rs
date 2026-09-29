use rustyraft::raft::state_machine::StateMachine;

struct TestStateMachine {
    applied: Vec<String>,
}

impl TestStateMachine {
    fn new() -> Self {
        Self {
            applied: Vec::new(),
        }
    }
}

impl StateMachine<String> for TestStateMachine {
    type Error = ();

    fn apply(
        &mut self,
        command: &String,
    ) -> Result<(), Self::Error> {
        self.applied.push(command.clone());
        Ok(())
    }
}

#[test]
fn state_machine_starts_empty() {
    let state_machine = TestStateMachine::new();

    assert!(state_machine.applied.is_empty());
}


#[test]
fn state_machine_applies_command() {
    let mut state_machine = TestStateMachine::new();

    state_machine
        .apply(&"A".to_string())
        .expect("command should apply");

    assert_eq!(
        state_machine.applied,
        vec!["A".to_string()]
    );
}

#[test]
fn state_machine_applies_commands_in_order() {
    let mut state_machine = TestStateMachine::new();

    state_machine
        .apply(&"A".to_string())
        .expect("command should apply");

    state_machine
        .apply(&"B".to_string())
        .expect("command should apply");

    state_machine
        .apply(&"C".to_string())
        .expect("command should apply");

    assert_eq!(
        state_machine.applied,
        vec![
            "A".to_string(),
            "B".to_string(),
            "C".to_string(),
        ]
    );
}

struct FailingStateMachine;

impl StateMachine<String> for FailingStateMachine {
    type Error = &'static str;

    fn apply(
        &mut self,
        _command: &String,
    ) -> Result<(), Self::Error> {
        Err("apply failed")
    }
}

#[test]
fn state_machine_returns_apply_error() {
    let mut state_machine = FailingStateMachine;

    let result = state_machine.apply(
        &"A".to_string()
    );

    assert_eq!(result, Err("apply failed"));
}