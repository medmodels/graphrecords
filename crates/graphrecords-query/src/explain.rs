use crate::Operand;
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

impl<O: Operand> Explain for O {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        self.context().describe(formatter)
    }
}

pub struct ExplainFormatter<'a, 'writer> {
    writer: &'writer mut dyn Write,
    children: Vec<&'a dyn Explain>,
}

impl<'a> ExplainFormatter<'a, '_> {
    pub fn child(&mut self, child: &'a dyn Explain) -> &mut Self {
        self.children.push(child);
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

    for (index, child) in children.into_iter().enumerate() {
        let last = index + 1 == count;

        write!(formatter, "\n{prefix}{}", if last { "└─ " } else { "├─ " })?;

        let mut child_prefix = String::from(prefix);
        child_prefix.push_str(if last { "   " } else { "│  " });
        write_node(child, formatter, &child_prefix)?;
    }

    Ok(())
}
