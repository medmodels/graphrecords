pub trait Trim {
    type ReturnOperand;

    fn trim(&self) -> Self::ReturnOperand;
}

pub trait TrimStart {
    type ReturnOperand;

    fn trim_start(&self) -> Self::ReturnOperand;
}

pub trait TrimEnd {
    type ReturnOperand;

    fn trim_end(&self) -> Self::ReturnOperand;
}

pub trait Lowercase {
    type ReturnOperand;

    fn lowercase(&self) -> Self::ReturnOperand;
}

pub trait Uppercase {
    type ReturnOperand;

    fn uppercase(&self) -> Self::ReturnOperand;
}

pub trait Slice {
    type ReturnOperand;

    fn slice(&self, start: usize, end: usize) -> Self::ReturnOperand;
}

pub trait Split<A> {
    type ReturnOperand;

    fn split(&self, delimiter: A) -> Self::ReturnOperand;
}

pub trait StartsWith<A> {
    type ReturnOperand;

    fn starts_with(&self, argument: A) -> Self::ReturnOperand;
}

pub trait EndsWith<A> {
    type ReturnOperand;

    fn ends_with(&self, argument: A) -> Self::ReturnOperand;
}

pub trait Contains<A> {
    type ReturnOperand;

    fn contains(&self, argument: A) -> Self::ReturnOperand;
}

pub trait Replace<A, B> {
    type ReturnOperand;

    fn replace(&self, old: A, new: B) -> Self::ReturnOperand;
}

pub trait ReplaceAll<A, B> {
    type ReturnOperand;

    fn replace_all(&self, old: A, new: B) -> Self::ReturnOperand;
}

pub trait Length {
    type ReturnOperand;

    fn length(&self) -> Self::ReturnOperand;
}

pub trait StripPrefix<A> {
    type ReturnOperand;

    fn strip_prefix(&self, prefix: A) -> Self::ReturnOperand;
}

pub trait StripSuffix<A> {
    type ReturnOperand;

    fn strip_suffix(&self, suffix: A) -> Self::ReturnOperand;
}

pub trait Reverse {
    type ReturnOperand;

    fn reverse(&self) -> Self::ReturnOperand;
}

pub trait PadStart<W, C> {
    type ReturnOperand;

    fn pad_start(&self, width: W, character: C) -> Self::ReturnOperand;
}

pub trait PadEnd<W, C> {
    type ReturnOperand;

    fn pad_end(&self, width: W, character: C) -> Self::ReturnOperand;
}

pub trait Matches<A> {
    type ReturnOperand;

    fn matches(&self, pattern: A) -> Self::ReturnOperand;
}
