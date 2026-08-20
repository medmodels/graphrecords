pub trait Trim {
    type Output;

    fn trim(&self) -> Self::Output;
}

pub trait TrimStart {
    type Output;

    fn trim_start(&self) -> Self::Output;
}

pub trait TrimEnd {
    type Output;

    fn trim_end(&self) -> Self::Output;
}

pub trait Lowercase {
    type Output;

    fn lowercase(&self) -> Self::Output;
}

pub trait Uppercase {
    type Output;

    fn uppercase(&self) -> Self::Output;
}

pub trait Slice {
    type Output;

    fn slice(&self, start: usize, end: usize) -> Self::Output;
}

pub trait Split<A> {
    type Output;

    fn split(&self, delimiter: A) -> Self::Output;
}

pub trait StartsWith<A> {
    type Output;

    fn starts_with(&self, argument: A) -> Self::Output;
}

pub trait EndsWith<A> {
    type Output;

    fn ends_with(&self, argument: A) -> Self::Output;
}

pub trait Contains<A> {
    type Output;

    fn contains(&self, argument: A) -> Self::Output;
}

pub trait Replace<A, B> {
    type Output;

    fn replace(&self, old: A, new: B) -> Self::Output;
}

pub trait ReplaceAll<A, B> {
    type Output;

    fn replace_all(&self, old: A, new: B) -> Self::Output;
}

pub trait Length {
    type Output;

    fn length(&self) -> Self::Output;
}

pub trait StripPrefix<A> {
    type Output;

    fn strip_prefix(&self, prefix: A) -> Self::Output;
}

pub trait StripSuffix<A> {
    type Output;

    fn strip_suffix(&self, suffix: A) -> Self::Output;
}

pub trait Reverse {
    type Output;

    fn reverse(&self) -> Self::Output;
}

pub trait PadStart<W, C> {
    type Output;

    fn pad_start(&self, width: W, character: C) -> Self::Output;
}

pub trait PadEnd<W, C> {
    type Output;

    fn pad_end(&self, width: W, character: C) -> Self::Output;
}

pub trait Matches<A> {
    type Output;

    fn matches(&self, pattern: A) -> Self::Output;
}
