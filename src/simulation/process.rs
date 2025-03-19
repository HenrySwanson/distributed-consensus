use super::Context;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProcessID(pub usize);

// TODO: generalize message and consensus types!
pub trait Process {
    type Message: std::fmt::Debug + Clone;

    fn new(id: ProcessID) -> Self;
    fn tick(&mut self, ctx: Context<Self::Message>);
    fn crash(&mut self);
    fn status(&self) -> String;
    fn decided_value(&self) -> Option<&String>;
}

impl std::fmt::Display for ProcessID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
