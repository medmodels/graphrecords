use crate::operations::EdgeDirection;

pub trait Edges {
    type Output;

    fn edges(&self, direction: EdgeDirection) -> Self::Output;
}

pub trait Neighbors {
    type Output;

    fn neighbors(&self, direction: EdgeDirection) -> Self::Output;
}

pub trait Nodes {
    type Output;

    fn nodes(&self) -> Self::Output;
}

pub trait SourceNode {
    type Output;

    fn source_node(&self) -> Self::Output;
}

pub trait TargetNode {
    type Output;

    fn target_node(&self) -> Self::Output;
}

pub trait ViaEdges {
    type Output;

    fn via_edges(&self, direction: EdgeDirection) -> Self::Output;
}

pub trait ViaNeighbors {
    type Output;

    fn via_neighbors(&self, direction: EdgeDirection) -> Self::Output;
}

pub trait ViaNodes {
    type Output;

    fn via_nodes(&self) -> Self::Output;
}

pub trait ViaSourceNode {
    type Output;

    fn via_source_node(&self) -> Self::Output;
}

pub trait ViaTargetNode {
    type Output;

    fn via_target_node(&self) -> Self::Output;
}
