use crate::operations::EdgeDirection;

pub trait Edges {
    type ReturnOperand;

    fn edges(&self, direction: EdgeDirection) -> Self::ReturnOperand;
}

pub trait Neighbors {
    type ReturnOperand;

    fn neighbors(&self, direction: EdgeDirection) -> Self::ReturnOperand;
}

pub trait Nodes {
    type ReturnOperand;

    fn nodes(&self) -> Self::ReturnOperand;
}

pub trait SourceNode {
    type ReturnOperand;

    fn source_node(&self) -> Self::ReturnOperand;
}

pub trait TargetNode {
    type ReturnOperand;

    fn target_node(&self) -> Self::ReturnOperand;
}

pub trait ViaEdges {
    type ReturnOperand;

    fn via_edges(&self, direction: EdgeDirection) -> Self::ReturnOperand;
}

pub trait ViaNeighbors {
    type ReturnOperand;

    fn via_neighbors(&self, direction: EdgeDirection) -> Self::ReturnOperand;
}

pub trait ViaNodes {
    type ReturnOperand;

    fn via_nodes(&self) -> Self::ReturnOperand;
}

pub trait ViaSourceNode {
    type ReturnOperand;

    fn via_source_node(&self) -> Self::ReturnOperand;
}

pub trait ViaTargetNode {
    type ReturnOperand;

    fn via_target_node(&self) -> Self::ReturnOperand;
}
