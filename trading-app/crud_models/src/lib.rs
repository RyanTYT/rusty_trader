extern crate proc_macro;
use crud_insertable::DeriveInsertable;
use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Type, parse_macro_input};

#[proc_macro_derive(ExtractPrimaryKeys)]
pub fn extract_primary_keys(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = &input.ident;
    let new_name = syn::Ident::new(&format!("{}PrimaryKeys", name), name.span());

    let data = match input.data {
        syn::Data::Struct(ref s) => s,
        _ => panic!("ExtractPrimaryKeys only works on Struct!"),
    };

    let primary_key_fields: Vec<_> = data
        .fields
        .iter()
        .filter_map(|field| {
            let serde_attrs: Vec<_> = field
                .attrs
                .iter()
                .filter(|attr| attr.path().is_ident("serde"))
                .cloned()
                .collect();

            if let Type::Path(ref type_path) = field.ty {
                if let Some(segment) = type_path.path.segments.last() {
                    if segment.ident != "Option" {
                        let field_name = &field.ident;
                        return Some(quote! {
                            #(#serde_attrs)*
                            pub #field_name : #type_path
                        });
                    }
                }
            }
            None
        })
        .collect();

    quote! {
    #[derive(
        Debug, Clone, Serialize, Deserialize, FromRow, DeriveInsertable
    )]
            pub struct #new_name {
               #(#primary_key_fields),*
            }
        }
    .into()
}

#[proc_macro_derive(ExtractFullKeys)]
pub fn extract_full_keys(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = &input.ident;
    let new_name = syn::Ident::new(&format!("{}FullKeys", name), name.span());

    let data = match input.data {
        syn::Data::Struct(ref s) => s,
        _ => panic!("ExtractFullKeys only works on Struct!"),
    };

    let full_key_fields: Vec<_> = data
        .fields
        .iter()
        .filter_map(|field| {
            let serde_attrs: Vec<_> = field
                .attrs
                .iter()
                .filter(|attr| attr.path().is_ident("serde"))
                .cloned()
                .collect();

            if let Type::Path(ref type_path) = field.ty {
                if let Some(segment) = type_path.path.segments.last() {
                    if segment.ident == "Option" {
                        // Extract type from within Option
                        if let syn::PathArguments::AngleBracketed(ref args) = segment.arguments {
                            if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
                                let field_name = &field.ident;
                                return Some(quote! {
                                    #(#serde_attrs)*
                                    pub #field_name : #inner_ty
                                });
                            }
                        }
                    }
                    let field_name = &field.ident;
                    return Some(quote! {
                        #(#serde_attrs)*
                        pub #field_name : #type_path
                    });
                }
            }
            None
        })
        .collect();

    quote! {
    #[derive(
        Debug, Clone, Serialize, Deserialize, FromRow, DeriveInsertable
    )]
            pub struct #new_name {
                #(#full_key_fields),*
            }
        }
    .into()
}

#[proc_macro_derive(ExtractUpdateKeys)]
pub fn extract_update_keys(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = &input.ident;
    let new_name = syn::Ident::new(&format!("{}UpdateKeys", name), name.span());

    let data = match input.data {
        syn::Data::Struct(ref s) => s,
        _ => panic!("ExtractUpdateKeys only works on Struct!"),
    };

    let update_key_fields: Vec<_> = data
        .fields
        .iter()
        .filter_map(|field| {
            let serde_attrs: Vec<_> = field
                .attrs
                .iter()
                .filter(|attr| attr.path().is_ident("serde"))
                .cloned()
                .collect();

            if let Type::Path(ref type_path) = field.ty {
                if let Some(segment) = type_path.path.segments.last() {
                    if segment.ident == "Option" {
                        let field_name = &field.ident;
                        return Some(quote! {
                            #(#serde_attrs)*
                            pub #field_name : #type_path
                        });
                    }
                }
            }
            None
        })
        .collect();

    quote! {
    #[derive(
        Debug, Clone, Serialize, Deserialize, FromRow, DeriveInsertable
    )]
            pub struct #new_name {
                #(#update_key_fields),*
            }
        }
    .into()
}
