use super::Context;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProcessID(pub usize);

// TODO: generalize message and consensus types!
pub trait Process {
    type Message: std::fmt::Debug + Clone;
    type Consensus: Merge + std::fmt::Debug + Clone + Eq;

    fn new(id: ProcessID) -> Self;

    fn tick(&mut self, ctx: Context<Self::Message>);
    fn crash(&mut self);

    fn status(&self) -> String;

    fn is_done(&self) -> bool;
    fn decided_value(&self) -> Self::Consensus;
}

// hey this is a lower semilattice!
pub trait Merge: Sized {
    fn empty() -> Self;
    fn merge(self, other: Self) -> Result<Self, (Self, Self)>;
}

impl std::fmt::Display for ProcessID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
