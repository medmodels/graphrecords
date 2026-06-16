pub trait Cache {
    #[must_use]
    fn cache(&self) -> Self;
}
