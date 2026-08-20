pub trait MemberEdges {
    type Output;

    fn edges(&self) -> Self::Output;
}

pub trait ViaMemberEdges {
    type Output;

    fn via_edges(&self) -> Self::Output;
}

pub trait Groups {
    type Output;

    fn groups(&self) -> Self::Output;
}

pub trait ViaGroups {
    type Output;

    fn via_groups(&self) -> Self::Output;
}

pub trait NodeCount {
    type Output;

    fn node_count(&self) -> Self::Output;
}

pub trait EdgeCount {
    type Output;

    fn edge_count(&self) -> Self::Output;
}
