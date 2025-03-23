use itertools::Itertools;
use rand::Rng;

use crate::paxos::Paxos;
use crate::paxos::PROPOSAL_COOLDOWN;
use crate::paxos::PROPOSAL_PROBABILITY;
use crate::simulation::Context;
use crate::simulation::Incoming;
use crate::simulation::Outgoing;
use crate::simulation::Process;
use crate::simulation::ProcessID;

const TARGET_LOG_SIZE: usize = 10;
const MAX_LOG_SIZE: usize = 30;

#[derive(Debug)]
pub struct MultiPaxos {
    id: ProcessID,
    instances: Vec<Paxos>,
    min_next_proposal_time: u64,
}

#[derive(Debug, Clone)]
pub struct Message {
    idx: usize,
    msg: crate::paxos::Message,
}

impl Process for MultiPaxos {
    type Message = Message;

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
            let instance = match self.instances.get_mut(msg.idx) {
                Some(x) => x,
                None => {
                    self.instances
                        .resize_with(msg.idx + 1, || Paxos::new(self.id));
                    self.instances.get_mut(msg.idx).unwrap()
                }
            };

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

    fn crash(&mut self) {
        for instance in &mut self.instances {
            instance.crash();
        }
    }

    fn status(&self) -> String {
        // TODO: make this much more detailed
        format!(
            "Process #{}: Decided values: {}",
            self.id,
            self.instances
                .iter()
                .map(|x| x.decided_value().unwrap_or(String::from("NONE")))
                .format(",")
        )
    }

    fn decided_value(&self) -> Option<String> {
        // TODO: do something different here when we can return different
        // consensus types
        let values: Option<Vec<_>> = self
            .instances
            .iter()
            .map(|x| x.decided_value())
            .chain(std::iter::repeat(None))
            .take(TARGET_LOG_SIZE)
            .collect();

        values.map(|values| values.iter().join(","))
    }
}
