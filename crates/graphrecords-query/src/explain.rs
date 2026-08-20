use crate::Expression;
pub use graphrecords_macros::Explain;
use std::fmt::{self, Display, Formatter, Write};

#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot explain itself",
    note = "implement `Explain` for `{Self}` or derive it with `#[derive(Explain)]`"
)]
pub trait Explain {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result;
}

pub trait Labeled {
    const LABEL: &'static str;
}

impl<E: Expression> Explain for E {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        self.context().describe(formatter)
    }
}

pub struct ExplainFormatter<'a, 'writer> {
    writer: &'writer mut dyn Write,
    children: Vec<(Option<&'static str>, &'a dyn Explain)>,
}

impl<'a> ExplainFormatter<'a, '_> {
    pub fn child(&mut self, child: &'a dyn Explain) -> &mut Self {
        self.children.push((None, child));
        self
    }

    pub fn labeled_child(&mut self, child: &'a dyn Explain, label: &'static str) -> &mut Self {
        self.children.push((Some(label), child));
        self
    }
}

impl Write for ExplainFormatter<'_, '_> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.writer.write_str(text)
    }
}

pub struct Explanation<'a> {
    root: &'a dyn Explain,
}

impl<'a> Explanation<'a> {
    #[must_use]
    pub fn new(root: &'a dyn Explain) -> Self {
        Self { root }
    }
}

impl Display for Explanation<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write_node(self.root, formatter, "")
    }
}

fn write_node(node: &dyn Explain, formatter: &mut Formatter<'_>, prefix: &str) -> fmt::Result {
    let children = {
        let mut explain_formatter = ExplainFormatter {
            writer: formatter,
            children: Vec::new(),
        };
        node.describe(&mut explain_formatter)?;

        explain_formatter.children
    };

    let count = children.len();

    for (index, (label, child)) in children.into_iter().enumerate() {
        let last = index + 1 == count;

        write!(formatter, "\n{prefix}{}", if last { "└─ " } else { "├─ " })?;

        if let Some(label) = label {
            write!(formatter, "{label}: ")?;
        }

        let mut child_prefix = String::from(prefix);
        child_prefix.push_str(if last { "   " } else { "│  " });
        write_node(child, formatter, &child_prefix)?;
    }

    Ok(())
}

const COMPACT_PLAN_WIDTH: usize = 60;

pub struct CompactPlan<'a> {
    root: &'a dyn Explain,
}

impl<'a> CompactPlan<'a> {
    #[must_use]
    pub fn new(root: &'a dyn Explain) -> Self {
        Self { root }
    }
}

impl Display for CompactPlan<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write_compact_node(self.root, formatter)
    }
}

fn write_compact_node<W: Write>(node: &dyn Explain, output: &mut W) -> fmt::Result {
    let mut header = String::new();
    let children = {
        let mut explain_formatter = ExplainFormatter {
            writer: &mut header,
            children: Vec::new(),
        };
        node.describe(&mut explain_formatter)?;

        explain_formatter.children
    };

    let mut predecessor = None;
    let mut arguments = Vec::new();

    for (label, child) in children {
        if label.is_none() && predecessor.is_none() {
            predecessor = Some(child);
        } else {
            arguments.push((label, child));
        }
    }

    if let Some(predecessor) = predecessor {
        write_compact_node(predecessor, output)?;
        output.write_str(" → ")?;
    }

    output.write_str(&header)?;

    for (label, argument) in arguments {
        let mut nested = String::new();
        write_compact_node(argument, &mut nested)?;

        match label {
            Some(label) => write!(output, " {label}=({nested})")?,
            None => write!(output, " ({nested})")?,
        }
    }

    Ok(())
}

pub(crate) fn write_truncated(formatter: &mut Formatter<'_>, plan: &str) -> fmt::Result {
    match plan.char_indices().nth(COMPACT_PLAN_WIDTH) {
        Some((boundary, _)) => write!(formatter, "{}…", &plan[..boundary]),
        None => formatter.write_str(plan),
    }
}

pub(crate) fn write_truncated_plan(
    formatter: &mut Formatter<'_>,
    root: &dyn Explain,
) -> fmt::Result {
    write_truncated(formatter, &CompactPlan::new(root).to_string())
}
