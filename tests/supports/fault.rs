use rustyraft::raft::ServerId;


pub struct DeterministicRng {
    state: u64,
}


impl DeterministicRng {

    const DEFAULT_STATE: u64 =
        0x9E3779B97F4A7C15;

    pub fn new(
        seed: u64,
    ) -> Self {
        let state =
            if seed == 0 {
                Self::DEFAULT_STATE
            } else {
                seed
            };

        Self {
            state,
        }
    }

    pub fn next_u64(
        &mut self,
    ) -> u64 {
        let mut value =
            self.state;

        value ^=
            value << 13;

        value ^=
            value >> 7;

        value ^=
            value << 17;

        self.state =
            value;

        value
    }

    pub fn next_usize(
        &mut self,
        upper_bound: usize,
    ) -> usize {
        assert!(
            upper_bound > 0,
            "upper bound should be greater than zero"
        );

        (self.next_u64()
            as usize)
            % upper_bound
    }
}


#[derive(Debug, PartialEq, Eq)]
pub enum FaultAction {
    Tick(ServerId),

    DeliverNext,

    DeliverAt(usize),

    DropTo(ServerId),
}


pub struct DeterministicFaultGenerator {
    rng: DeterministicRng,
    server_ids: Vec<ServerId>,
}


impl DeterministicFaultGenerator {

    pub fn new(
        seed: u64,
        server_ids: &[ServerId],
    ) -> Self {
        assert!(
            !server_ids.is_empty(),
            "fault generator needs at least one server"
        );

        Self {
            rng: DeterministicRng::new(
                seed,
            ),
            server_ids:
                server_ids.to_vec(),
        }
    }

    pub fn next(
        &mut self,
    ) -> FaultAction {
        let action =
            self.rng.next_usize(4);

        match action {
            0 => {
                let server =
                    self.random_server();

                FaultAction::Tick(
                    server,
                )
            }

            1 => {
                FaultAction::DeliverNext
            }

            2 => {
                let position =
                    self.rng.next_usize(8);

                FaultAction::DeliverAt(
                    position,
                )
            }

            3 => {
                let server =
                    self.random_server();

                FaultAction::DropTo(
                    server,
                )
            }

            _ => {
                unreachable!()
            }
        }
    }

    pub fn generate(
        &mut self,
        count: usize,
    ) -> Vec<FaultAction> {
        let mut actions =
            Vec::with_capacity(
                count,
            );

        for _ in 0..count {
            actions.push(
                self.next()
            );
        }

        actions
    }

    fn random_server(
        &mut self,
    ) -> ServerId {
        let index =
            self.rng.next_usize(
                self.server_ids.len()
            );

        self.server_ids[index]
    }
}