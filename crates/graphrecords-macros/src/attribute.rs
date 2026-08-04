use syn::{Attribute, Result, meta::ParseNestedMeta};

pub trait FromAttributes: Default {
    const NAMESPACE: &'static str;

    fn parse_meta(&mut self, meta: ParseNestedMeta) -> Result<()>;

    fn from_attributes(attributes: &[Attribute]) -> Result<Self> {
        let mut parsed = Self::default();

        for attribute in attributes {
            if attribute.path().is_ident(Self::NAMESPACE) {
                attribute.parse_nested_meta(|meta| parsed.parse_meta(meta))?;
            }
        }

        Ok(parsed)
    }
}
