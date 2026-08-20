use crate::{
    Diagnostic,
    registry::{
        ArgumentDescriptor, ArgumentMissingPolicy, ArgumentValueSource, ArityDescriptor,
        ExpressionDescriptor, IndexDescriptor, LaneShapeDescriptor, OrderDescriptor,
        ValueDescriptor, ValueRole,
    },
};
use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

struct TypeName(&'static str);

impl Display for TypeName {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0.rsplit_once("::").map_or(self.0, |split| split.1))
    }
}

struct IndexState<'a>(&'a IndexDescriptor);

impl Display for IndexState<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self.0 {
            IndexDescriptor::Domain(domain) => TypeName(domain.name()).fmt(formatter),
            IndexDescriptor::Expanded { parent, child } => {
                write!(formatter, "{} expanded by {}", Self(parent), Self(child))
            }
            IndexDescriptor::ExpandedParent { parent } => {
                write!(formatter, "{} expansion sources", Self(parent))
            }
        }
    }
}

struct ValueState<'a>(&'a ValueDescriptor);

impl Display for ValueState<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self.0.role() {
            ValueRole::Value => write!(formatter, "{} values", TypeName(self.0.domain().name())),
            ValueRole::Index(index) => write!(formatter, "{} index values", IndexState(index)),
            ValueRole::EntityReference(index) => {
                write!(formatter, "{} entity references", IndexState(index))
            }
            ValueRole::Unit => formatter.write_str("unit values"),
        }
    }
}

struct ExpressionState<'a>(&'a ExpressionDescriptor);

impl Display for ExpressionState<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self.0 {
            ExpressionDescriptor::Lane { shape, arity } => {
                let arity = match arity {
                    ArityDescriptor::Multiple {
                        order: OrderDescriptor::Ordered,
                    } => "an ordered",
                    ArityDescriptor::Multiple {
                        order: OrderDescriptor::Unordered,
                    } => "an unordered",
                    ArityDescriptor::Single => "a single-element",
                    ArityDescriptor::Definite => "a definite",
                };

                match shape {
                    LaneShapeDescriptor::Indexed { index, value } => write!(
                        formatter,
                        "{arity} values of {} indexed by {}",
                        ValueState(value),
                        IndexState(index)
                    ),
                    LaneShapeDescriptor::Bare { value } => {
                        write!(formatter, "{arity} bare values of {}", ValueState(value))
                    }
                }
            }
            ExpressionDescriptor::Group {
                member,
                key,
                payload,
            } => write!(
                formatter,
                "a group of {} members keyed by {} containing {}",
                IndexState(member),
                IndexState(key),
                Self(payload)
            ),
        }
    }
}

struct ArgumentSourceState<'a>(&'a ArgumentValueSource);

impl Display for ArgumentSourceState<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self.0 {
            ArgumentValueSource::Literal(value) => {
                write!(formatter, "a literal of {}", ValueState(value))
            }
            ArgumentValueSource::Expression(expression) => {
                write!(formatter, "values from {}", ExpressionState(expression))
            }
        }
    }
}

struct ArgumentState<'a>(&'a ArgumentDescriptor);

impl Display for ArgumentState<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self.0 {
            ArgumentDescriptor::Value(argument) => {
                ArgumentSourceState(argument.source()).fmt(formatter)?;

                match argument.missing() {
                    ArgumentMissingPolicy::None => Ok(()),
                    ArgumentMissingPolicy::Drop => formatter.write_str(" dropping missing values"),
                    ArgumentMissingPolicy::Replace(replacement) => write!(
                        formatter,
                        " replacing missing values with {}",
                        ArgumentSourceState(replacement)
                    ),
                }
            }
            ArgumentDescriptor::Field(domain) => {
                write!(formatter, "a field of type {}", TypeName(domain.name()))
            }
            ArgumentDescriptor::Selector(domain) => {
                write!(formatter, "a selector of type {}", TypeName(domain.name()))
            }
            ArgumentDescriptor::Expression(expression) => {
                ExpressionState(expression).fmt(formatter)
            }
        }
    }
}

#[derive(Debug)]
pub struct OperationNotApplicable {
    method: String,
    input: ExpressionDescriptor,
    arguments: Vec<ArgumentDescriptor>,
}

impl OperationNotApplicable {
    #[must_use]
    pub const fn new(
        method: String,
        input: ExpressionDescriptor,
        arguments: Vec<ArgumentDescriptor>,
    ) -> Self {
        Self {
            method,
            input,
            arguments,
        }
    }

    #[must_use]
    pub fn method(&self) -> &str {
        &self.method
    }

    #[must_use]
    pub const fn input(&self) -> &ExpressionDescriptor {
        &self.input
    }

    #[must_use]
    pub fn arguments(&self) -> &[ArgumentDescriptor] {
        &self.arguments
    }
}

impl Display for OperationNotApplicable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "operation `{}` is not applicable to {}",
            self.method,
            ExpressionState(&self.input)
        )?;

        let Some(first) = self.arguments.first() else {
            return formatter.write_str(" without arguments");
        };

        write!(formatter, " with arguments {}", ArgumentState(first))?;

        self.arguments
            .iter()
            .skip(1)
            .try_for_each(|argument| write!(formatter, ", {}", ArgumentState(argument)))
    }
}

impl Error for OperationNotApplicable {}

impl Diagnostic for OperationNotApplicable {
    fn name() -> &'static str {
        "OperationNotApplicable"
    }

    fn help(&self) -> Option<String> {
        Some("choose an operation and arguments supported by the expression descriptor".to_string())
    }
}

#[derive(Debug)]
pub struct UnsupportedValueRole {
    capability: &'static str,
    role: &'static str,
}

impl UnsupportedValueRole {
    #[must_use]
    pub const fn new(capability: &'static str, role: &'static str) -> Self {
        Self { capability, role }
    }

    #[must_use]
    pub const fn capability(&self) -> &'static str {
        self.capability
    }

    #[must_use]
    pub const fn role(&self) -> &'static str {
        self.role
    }
}

impl Display for UnsupportedValueRole {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "a {} value does not support `{}`",
            self.role, self.capability
        )
    }
}

impl Error for UnsupportedValueRole {}

impl Diagnostic for UnsupportedValueRole {
    fn name() -> &'static str {
        "UnsupportedValueRole"
    }

    fn help(&self) -> Option<String> {
        Some(format!(
            "handle the failing elements with `on_error(...)` before using them for {}",
            self.capability
        ))
    }
}
