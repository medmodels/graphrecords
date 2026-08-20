use super::optimizer::plan::with_bounds;
use crate::{attribute::FromAttributes, query::resolve_query_crate_path};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Data, DeriveInput, Error, Generics, Ident, Index, LitStr, Path, Result, Type,
    meta::ParseNestedMeta,
};

#[derive(Default)]
struct ExplainAttributes {
    crate_path: Option<Path>,
    label: Option<String>,
}

impl FromAttributes for ExplainAttributes {
    const NAMESPACE: &'static str = "explain";

    fn parse_meta(&mut self, meta: ParseNestedMeta) -> Result<()> {
        if meta.path.is_ident("crate") {
            self.crate_path = Some(meta.value()?.parse::<LitStr>()?.parse()?);
        } else if meta.path.is_ident("label") {
            self.label = Some(meta.value()?.parse::<LitStr>()?.value());
        } else {
            return Err(meta.error("unknown explain attribute"));
        }

        Ok(())
    }
}

#[derive(Default)]
struct ExplainFieldAttributes {
    label: bool,
    inline: bool,
}

impl FromAttributes for ExplainFieldAttributes {
    const NAMESPACE: &'static str = "explain";

    fn parse_meta(&mut self, meta: ParseNestedMeta) -> Result<()> {
        if meta.path.is_ident("label") {
            self.label = true;
            Ok(())
        } else if meta.path.is_ident("inline") {
            self.inline = true;
            Ok(())
        } else {
            Err(meta.error("unknown explain attribute"))
        }
    }
}

struct ExplainModel {
    ident: Ident,
    generics: Generics,
    crate_path: Path,
    node_label: String,
    label_names: Vec<String>,
    label_accessors: Vec<TokenStream>,
    inline_accessors: Vec<TokenStream>,
    child_accessors: Vec<TokenStream>,
    argument_names: Vec<String>,
    argument_accessors: Vec<TokenStream>,
    argument_types: Vec<Type>,
}

impl ExplainModel {
    fn parse(input: &DeriveInput) -> Result<Self> {
        let Data::Struct(data) = &input.data else {
            return Err(Error::new_spanned(
                input,
                "Explain can only be derived for structs",
            ));
        };

        let attributes = ExplainAttributes::from_attributes(&input.attrs)?;

        let crate_path = match attributes.crate_path {
            Some(path) => path,
            None => resolve_query_crate_path()?,
        };

        let node_label = attributes.label.unwrap_or_else(|| input.ident.to_string());

        let mut label_names = Vec::new();
        let mut label_accessors = Vec::new();
        let mut inline_accessors = Vec::new();
        let mut child_accessors = Vec::new();
        let mut argument_names = Vec::new();
        let mut argument_accessors = Vec::new();
        let mut argument_types = Vec::new();

        for (index, field) in data.fields.iter().enumerate() {
            let accessor = if let Some(name) = &field.ident {
                quote!(#name)
            } else {
                let index = Index::from(index);
                quote!(#index)
            };

            let is_input = field
                .attrs
                .iter()
                .any(|attribute| attribute.path().is_ident("input"));

            let is_argument = field
                .attrs
                .iter()
                .any(|attribute| attribute.path().is_ident("argument"));

            let ExplainFieldAttributes { label, inline } =
                ExplainFieldAttributes::from_attributes(&field.attrs)?;

            if label && inline {
                return Err(Error::new_spanned(
                    field,
                    "a field cannot be both `#[explain(label)]` and `#[explain(inline)]`",
                ));
            }

            if is_input {
                if label || inline {
                    return Err(Error::new_spanned(
                        field,
                        "a field cannot be both `#[input]` and `#[explain(...)]`",
                    ));
                }

                child_accessors.push(accessor);
            } else if is_argument {
                if label || inline {
                    return Err(Error::new_spanned(
                        field,
                        "a field cannot be both `#[argument]` and `#[explain(...)]`",
                    ));
                }

                let name = match &field.ident {
                    Some(name) => name.to_string(),
                    None => index.to_string(),
                };

                argument_names.push(name);
                argument_accessors.push(accessor);
                argument_types.push(field.ty.clone());
            } else if label {
                let name = match &field.ident {
                    Some(name) => name.to_string(),
                    None => index.to_string(),
                };

                label_names.push(name);
                label_accessors.push(accessor);
            } else if inline {
                inline_accessors.push(accessor);
            }
        }

        Ok(Self {
            ident: input.ident.clone(),
            generics: input.generics.clone(),
            crate_path,
            node_label,
            label_names,
            label_accessors,
            inline_accessors,
            child_accessors,
            argument_names,
            argument_accessors,
            argument_types,
        })
    }
}

pub fn expand(input: &DeriveInput) -> Result<TokenStream> {
    let ExplainModel {
        ident,
        generics,
        crate_path,
        node_label,
        label_names,
        label_accessors,
        inline_accessors,
        child_accessors,
        argument_names,
        argument_accessors,
        argument_types,
    } = ExplainModel::parse(input)?;

    let generics = with_bounds(
        &generics,
        &argument_types,
        &quote!(#crate_path::explain::Explain),
    );

    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics #crate_path::explain::Explain for #ident #type_generics #where_clause {
            fn describe<'__explain>(
                &'__explain self,
                formatter: &mut #crate_path::explain::ExplainFormatter<'__explain, '_>,
            ) -> ::core::fmt::Result {
                ::core::fmt::Write::write_str(formatter, #node_label)?;
                #(
                    ::core::fmt::Write::write_fmt(
                        formatter,
                        ::core::format_args!(" {}={}", #label_names, &self.#label_accessors),
                    )?;
                )*
                #(
                    ::core::fmt::Write::write_fmt(
                        formatter,
                        ::core::format_args!(" {}", &self.#inline_accessors),
                    )?;
                )*
                #( formatter.child(&self.#child_accessors); )*
                #( formatter.labeled_child(&self.#argument_accessors, #argument_names); )*
                ::core::result::Result::Ok(())
            }
        }

        #[automatically_derived]
        impl #impl_generics #crate_path::explain::Labeled for #ident #type_generics #where_clause {
            const LABEL: &'static str = #node_label;
        }
    })
}
