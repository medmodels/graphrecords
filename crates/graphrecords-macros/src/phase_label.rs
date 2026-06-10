use crate::{attribute::FromAttributes, resolve_query_crate_path};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Generics, Ident, LitStr, Path, Result, meta::ParseNestedMeta};

#[derive(Default)]
struct PhaseLabelAttributes {
    crate_path: Option<Path>,
}

impl FromAttributes for PhaseLabelAttributes {
    const NAMESPACE: &'static str = "phase_label";

    fn parse_meta(&mut self, meta: ParseNestedMeta) -> Result<()> {
        if meta.path.is_ident("crate") {
            self.crate_path = Some(meta.value()?.parse::<LitStr>()?.parse()?);
            Ok(())
        } else {
            Err(meta.error("unknown phase_label attribute"))
        }
    }
}

struct PhaseLabelModel {
    ident: Ident,
    generics: Generics,
    crate_path: Path,
}

impl PhaseLabelModel {
    fn parse(input: &DeriveInput) -> Result<Self> {
        let crate_path = match PhaseLabelAttributes::from_attributes(&input.attrs)?.crate_path {
            Some(path) => path,
            None => resolve_query_crate_path()?,
        };

        Ok(Self {
            ident: input.ident.clone(),
            generics: input.generics.clone(),
            crate_path,
        })
    }
}

pub fn expand(input: &DeriveInput) -> Result<TokenStream> {
    let PhaseLabelModel {
        ident,
        generics,
        crate_path,
    } = PhaseLabelModel::parse(input)?;

    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics #crate_path::optimizer::PhaseLabel for #ident #type_generics #where_clause {
            fn dyn_clone(&self) -> ::std::boxed::Box<dyn #crate_path::optimizer::PhaseLabel> {
                ::std::boxed::Box::new(::core::clone::Clone::clone(self))
            }

            fn dyn_eq(&self, other: &dyn #crate_path::optimizer::PhaseLabel) -> bool {
                match #crate_path::optimizer::PhaseLabel::as_any(other).downcast_ref::<Self>() {
                    ::core::option::Option::Some(other) => self == other,
                    ::core::option::Option::None => false,
                }
            }

            fn dyn_hash(&self, mut state: &mut dyn ::core::hash::Hasher) {
                ::core::hash::Hash::hash(&::core::any::TypeId::of::<Self>(), &mut state);
                ::core::hash::Hash::hash(self, &mut state);
            }

            fn dyn_debug(&self, formatter: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                ::core::fmt::Debug::fmt(self, formatter)
            }

            fn as_any(&self) -> &dyn ::core::any::Any {
                self
            }
        }
    })
}
