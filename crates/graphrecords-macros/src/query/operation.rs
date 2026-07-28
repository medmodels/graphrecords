use super::optimizer::plan::PlanModel;
use crate::attribute::FromAttributes;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Error, Ident, Result, meta::ParseNestedMeta, parse_quote};

#[derive(Default)]
struct OperationAttributes {
    scope: Option<Ident>,
}

impl FromAttributes for OperationAttributes {
    const NAMESPACE: &'static str = "operation";

    fn parse_meta(&mut self, meta: ParseNestedMeta) -> Result<()> {
        if !meta.path.is_ident("scope") {
            return Err(meta.error("unknown operation attribute"));
        }

        if self.scope.is_some() {
            return Err(meta.error("duplicate operation scope"));
        }

        let scope = meta.value()?.parse::<Ident>()?;
        if !matches!(scope.to_string().as_str(), "Element" | "Lane" | "Group") {
            return Err(Error::new(
                scope.span(),
                "operation scope must be `Element`, `Lane`, or `Group`",
            ));
        }

        self.scope = Some(scope);
        Ok(())
    }
}

pub fn expand(input: &DeriveInput) -> Result<TokenStream> {
    let PlanModel {
        ident,
        mut generics,
        crate_path,
        ..
    } = PlanModel::parse(input)?;
    let scope = OperationAttributes::from_attributes(&input.attrs)?
        .scope
        .ok_or_else(|| {
            Error::new_spanned(
                input,
                "missing `#[operation(scope = ...)]`, expected `Element`, `Lane`, or `Group`",
            )
        })?;

    let bound = quote! {
        #crate_path::operations::Prepare
            + #crate_path::optimizer::OperationInputs
            + #crate_path::Explain
    };

    generics
        .make_where_clause()
        .predicates
        .push(parse_quote!(Self: #bound));

    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics #crate_path::operations::Operation
            for #ident #type_generics #where_clause
        {
            type Scope = #crate_path::operations::#scope;
        }
    })
}
