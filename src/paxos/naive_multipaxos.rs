use itertools::Itertools;
use rand::Rng;

use super::get_mut_extending_if_necessary;
use crate::paxos::Paxos;
use crate::paxos::PROPOSAL_COOLDOWN;
use crate::paxos::PROPOSAL_PROBABILITY;
use crate::simulation::Context;
use crate::simulation::Incoming;
use crate::simulation::Merge;
use crate::simulation::Outgoing;
use crate::simulation::Process;
use crate::simulation::ProcessID;

const TARGET_LOG_SIZE: usize = 10;
// TODO: if this is different it messes with the Consensus decision
const MAX_LOG_SIZE: usize = TARGET_LOG_SIZE;

#[derive(Debug)]
pub struct NaiveMultiPaxos {
    id: ProcessID,
    instances: Vec<Paxos>,
    min_next_proposal_time: u64,
}

#[derive(Debug, Clone)]
pub struct Message {
    idx: usize,
    msg: super::single_decree::Message,
}

impl Process for NaiveMultiPaxos {
    type Message = Message;
    type Consensus = Vec<Option<String>>;

    fn new(id: ProcessID) -> Self {
        Self {
            id,
            instances: vec![],
            min_next_proposal_time: 0,
        }
    }

    fn tick(&mut self, ctx: Context<Self::Message>) {
        // Apply the incoming messages, extending `self.instances` as necessary.
        for Incoming { from, msg } in ctx.received_messages {
            let idx = msg.idx;
            let instance = get_mut_extending_if_necessary(&mut self.instances, msg.idx, || {
                Paxos::new(self.id)
            });

            ctx.outgoing_messages.extend(
                instance
                    .recv_message(Incoming { from, msg: msg.msg }, ctx.current_tick)
                    .into_iter()
                    .map(|msg| Outgoing {
                        to: msg.to,
                        msg: Message { idx, msg: msg.msg },
                    }),
            );
        }

        // Now see if any of them want to send a new proposal
        for (idx, instance) in self.instances.iter_mut().enumerate() {
            if instance.decided_value().is_none()
                && instance.min_next_proposal_time <= ctx.current_tick
                && ctx.rng.random_bool(PROPOSAL_PROBABILITY)
            {
                log::trace!("Random proposal for slot {} from {}!", idx, self.id.0);
                let proposal_msgs = instance.create_proposal_messages(ctx.current_tick);
                ctx.outgoing_messages
                    .extend(proposal_msgs.into_iter().map(|msg| Outgoing {
                        to: msg.to,
                        msg: Message { idx, msg: msg.msg },
                    }));
            }
        }

        // Lastly, consider sending a proposal for a new value
        if self.instances.len() < MAX_LOG_SIZE
            && self.min_next_proposal_time < ctx.current_tick
            && ctx.rng.random_bool(PROPOSAL_PROBABILITY)
        {
            let new_idx = self.instances.len();
            let mut new_instance = Paxos::new(self.id);
            let proposal_msgs = new_instance.create_proposal_messages(ctx.current_tick);
            ctx.outgoing_messages
                .extend(proposal_msgs.into_iter().map(|msg| Outgoing {
                    to: msg.to,
                    msg: Message {
                        idx: new_idx,
                        msg: msg.msg,
                    },
                }));
            self.instances.push(new_instance);
            self.min_next_proposal_time = ctx.current_tick + PROPOSAL_COOLDOWN;
        }
    }

    fn restore_from_crash(&mut self, current_tick: u64) {
        for instance in &mut self.instances {
            instance.restore_from_crash(current_tick);
        }
        self.min_next_proposal_time = current_tick + PROPOSAL_COOLDOWN;
    }

    fn status(&self) -> String {
        format!(
            "Process #{}: Log: {}",
            self.id,
            self.instances
                .iter()
                .map(|x| x.decided_value().unwrap_or(String::from("NONE")))
                .format(",")
        )
    }

    fn is_done(&self) -> bool {
        self.decided_value()
            .into_iter()
            .chain(std::iter::repeat(None))
            .take(TARGET_LOG_SIZE)
            .all(|x| x.is_some())
    }

    fn decided_value(&self) -> Vec<Option<String>> {
        self.instances.iter().map(|x| x.decided_value()).collect()
    }
}

impl Merge for Vec<Option<String>> {
    fn empty() -> Self {
        vec![]
    }

    fn merge(self, other: Self) -> Result<Self, (Self, Self)> {
        let result: Option<Vec<_>> = self
            .clone()
            .into_iter()
            .zip_longest(other.clone())
            .map(|z| match z {
                itertools::EitherOrBoth::Both(x, y) => x.merge(y).ok(),
                itertools::EitherOrBoth::Left(x) => Some(x),
                itertools::EitherOrBoth::Right(y) => Some(y),
            })
            .collect();
        match result {
            Some(vec) => Ok(vec),
            None => Err((self, other)),
        }
    }
}
