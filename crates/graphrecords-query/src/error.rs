use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    sync::Arc,
};

pub type QueryResult<T> = Result<T, Box<Failure>>;

#[derive(Clone, Debug)]
pub struct Failure {
    operation: &'static str,
    element: Option<String>,
    help: Option<String>,
    cause: Arc<dyn Error + Send + Sync>,
}

impl Failure {
    pub fn new(operation: &'static str, cause: impl Error + Send + Sync + 'static) -> Box<Self> {
        Box::new(Self {
            operation,
            element: None,
            help: None,
            cause: Arc::new(cause),
        })
    }

    #[must_use]
    pub fn at(mut self: Box<Self>, element: impl Display) -> Box<Self> {
        self.element = Some(element.to_string());
        self
    }

    #[must_use]
    pub fn help(mut self: Box<Self>, help: impl Display) -> Box<Self> {
        self.help = Some(help.to_string());
        self
    }

    #[must_use]
    pub const fn operation(&self) -> &'static str {
        self.operation
    }

    #[must_use]
    pub fn element(&self) -> Option<&str> {
        self.element.as_deref()
    }

    #[must_use]
    pub fn cause(&self) -> &(dyn Error + Send + Sync) {
        self.cause.as_ref()
    }

    #[must_use]
    pub fn downcast_cause<T: Error + 'static>(&self) -> Option<&T> {
        self.cause.downcast_ref::<T>()
    }

    #[must_use]
    pub fn is_cause<T: Error + 'static>(&self) -> bool {
        self.cause.is::<T>()
    }
}

impl Display for Failure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match &self.element {
            Some(element) => write!(
                formatter,
                "operation `{}` failed at element \"{}\": {}",
                self.operation, element, self.cause,
            )?,
            None => write!(
                formatter,
                "operation `{}` failed: {}",
                self.operation, self.cause,
            )?,
        }

        if let Some(help) = &self.help {
            write!(formatter, "\nhelp: {help}")?;
        }

        Ok(())
    }
}

impl Error for Failure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.cause.as_ref())
    }
}

#[derive(Debug)]
pub struct IncomparableValues<T: Display> {
    pub first: T,
    pub second: T,
}

impl<T: Display> Display for IncomparableValues<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cannot compare values `{}` and `{}`",
            self.first, self.second
        )
    }
}

impl<T: Display + Debug> Error for IncomparableValues<T> {}
