use crate::{attribute::FromAttributes, resolve_query_crate_path};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Data, DeriveInput, Error, Generics, Ident, Index, LitStr, Path, Result, Type,
    meta::ParseNestedMeta,
};

#[derive(Default)]
struct OperandAttributes {
    crate_path: Option<Path>,
}

impl FromAttributes for OperandAttributes {
    const NAMESPACE: &'static str = "operand";

    fn parse_meta(&mut self, meta: ParseNestedMeta) -> Result<()> {
        if meta.path.is_ident("crate") {
            self.crate_path = Some(meta.value()?.parse::<LitStr>()?.parse()?);
            Ok(())
        } else {
            Err(meta.error("unknown operand attribute"))
        }
    }
}

#[derive(Default)]
struct OperandFieldAttributes {
    context: bool,
}

impl FromAttributes for OperandFieldAttributes {
    const NAMESPACE: &'static str = "operand";

    fn parse_meta(&mut self, meta: ParseNestedMeta) -> Result<()> {
        if meta.path.is_ident("context") {
            self.context = true;
            Ok(())
        } else {
            Err(meta.error("unknown operand attribute"))
        }
    }
}

struct OperandModel {
    ident: Ident,
    generics: Generics,
    crate_path: Path,
    context_index: usize,
    context_name: Option<Ident>,
    context_type: Type,
}

impl OperandModel {
    fn parse(input: &DeriveInput) -> Result<Self> {
        let crate_path = match OperandAttributes::from_attributes(&input.attrs)?.crate_path {
            Some(path) => path,
            None => resolve_query_crate_path()?,
        };

        let Data::Struct(data) = &input.data else {
            return Err(Error::new_spanned(
                input,
                "Operand can only be derived for structs",
            ));
        };

        let mut context = None;

        for (index, field) in data.fields.iter().enumerate() {
            if !OperandFieldAttributes::from_attributes(&field.attrs)?.context {
                continue;
            }

            if context.is_some() {
                return Err(Error::new_spanned(
                    &field.ty,
                    "only one field may be marked `#[operand(context)]`",
                ));
            }

            context = Some((index, field));
        }

        let Some((index, field)) = context else {
            return Err(Error::new_spanned(
                input,
                "`#[derive(Operand)]` requires one field marked `#[operand(context)]`",
            ));
        };

        Ok(Self {
            ident: input.ident.clone(),
            generics: input.generics.clone(),
            crate_path,
            context_index: index,
            context_name: field.ident.clone(),
            context_type: field.ty.clone(),
        })
    }
}

pub fn expand(input: &DeriveInput) -> Result<TokenStream> {
    let OperandModel {
        ident,
        generics,
        crate_path,
        context_index,
        context_name,
        context_type,
    } = OperandModel::parse(input)?;

    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    let (accessor, from_context) = if let Some(name) = context_name {
        (
            quote!(#name),
            quote! {
                fn from_context(#name: ::std::sync::Arc<Self::Context>) -> Self {
                    Self { #name }
                }
            },
        )
    } else {
        let index = Index::from(context_index);
        (
            quote!(#index),
            quote! {
                fn from_context(context: ::std::sync::Arc<Self::Context>) -> Self {
                    Self(context)
                }
            },
        )
    };

    Ok(quote! {
        impl #impl_generics #crate_path::Operand for #ident #type_generics #where_clause {
            type Context = <#context_type as ::core::ops::Deref>::Target;

            fn context(&self) -> &Self::Context {
                self.#accessor.as_ref()
            }

            fn as_plan_node(&self) -> &dyn #crate_path::optimizer::PlanNode {
                self.#accessor.as_ref()
            }

            #from_context
        }
    })
}
