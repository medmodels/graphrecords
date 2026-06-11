#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum EdgeDirection {
    Incoming,
    Outgoing,
    Both,
}

pub trait Edges {
    type ReturnOperand;

    fn edges(&self, direction: EdgeDirection) -> Self::ReturnOperand;
}

pub trait Neighbors {
    type ReturnOperand;

    fn neighbors(&self, direction: EdgeDirection) -> Self::ReturnOperand;
}

pub trait SourceNode {
    type ReturnOperand;

    fn source_node(&self) -> Self::ReturnOperand;
}

pub trait TargetNode {
    type ReturnOperand;

    fn target_node(&self) -> Self::ReturnOperand;
}
