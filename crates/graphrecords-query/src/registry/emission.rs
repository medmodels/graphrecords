use super::{
    descriptor::{ArityDescriptor, RetentionDescriptor},
    manifest::describe::{DescribeArity, DescribeEmission},
};
use crate::{
    Arity, Definite, Multiple, Ordered, Single, Unordered,
    element::{Dropping, ElementEmission, Expanding, Preserving},
};
use graphrecords_utils::aliases::GrHashMap;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum EmissionKind {
    Preserving,
    Dropping,
    ExpandingOrdered,
    ExpandingUnordered,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum EmissionSpec {
    Fixed(EmissionKind),
    OfArgument,
}

#[derive(Default)]
pub struct OutArityTable {
    cells: GrHashMap<(EmissionSpec, ArityDescriptor), ArityDescriptor>,
}

impl OutArityTable {
    pub(super) fn builtins() -> Self {
        let mut table = Self::default();

        table.insert_row::<Preserving>();
        table.insert_row::<Dropping>();
        table.insert_row::<Expanding<Ordered>>();
        table.insert_row::<Expanding<Unordered>>();

        table
    }

    fn insert_row<E>(&mut self)
    where
        E: ElementEmission + DescribeEmission,
        E::OutArity<Multiple<Ordered>>: DescribeArity,
        E::OutArity<Multiple<Unordered>>: DescribeArity,
        E::OutArity<Single>: DescribeArity,
        E::OutArity<Definite>: DescribeArity,
    {
        self.insert_cell::<E, Multiple<Ordered>>();
        self.insert_cell::<E, Multiple<Unordered>>();
        self.insert_cell::<E, Single>();
        self.insert_cell::<E, Definite>();
    }

    fn insert_cell<E, C>(&mut self)
    where
        E: ElementEmission + DescribeEmission,
        C: Arity + DescribeArity,
        E::OutArity<C>: DescribeArity,
    {
        self.cells.insert(
            (E::emission_spec(), C::arity_descriptor()),
            <E::OutArity<C> as DescribeArity>::arity_descriptor(),
        );
    }

    pub(super) fn resolve(
        &self,
        emission: EmissionSpec,
        arity: ArityDescriptor,
        retention: RetentionDescriptor,
    ) -> ArityDescriptor {
        let emission = match emission {
            EmissionSpec::Fixed(_) => emission,
            EmissionSpec::OfArgument => EmissionSpec::Fixed(match retention {
                RetentionDescriptor::Preserving => EmissionKind::Preserving,
                RetentionDescriptor::Dropping => EmissionKind::Dropping,
            }),
        };

        *self.cells.get(&(emission, arity)).unwrap_or_else(|| {
            panic!("registry has no materialized out arity for {emission:?} applied to {arity:?}")
        })
    }
}
