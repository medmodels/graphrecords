use crate::crate_path;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Result};

pub fn expand(input: &DeriveInput) -> Result<TokenStream> {
    let name = &input.ident;

    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();

    let crate_path = crate_path(&input.attrs, "phase_label", |_meta| Ok(false))?;

    Ok(quote! {
        impl #impl_generics #crate_path::optimizer::PhaseLabel for #name #type_generics #where_clause {
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
