use convert_case::Casing;
use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, parse_macro_input};

#[proc_macro_derive(DeriveInsertable)]
pub fn derive_insertable(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let struct_name = &input.ident;
    let table_name = struct_name.to_string().to_case(convert_case::Case::Snake); // `convert_case` crate

    let fields = match input.data {
        syn::Data::Struct(ref data_struct) => &data_struct.fields,
        _ => panic!("Insertable can only be derived for structs"),
    };

    let pri_field_names: Vec<_> = fields
        .iter()
        .filter_map(|field| {
            if let syn::Type::Path(type_path) = &field.ty {
                if type_path
                    .path
                    .segments
                    .iter()
                    .any(|seg| seg.ident == "Option")
                {
                    return None;
                }
                return Some(field.ident.as_ref().unwrap());
            }
            None
        })
        .collect();
    let opt_field_names: Vec<_> = fields
        .iter()
        .filter_map(|field| {
            if let syn::Type::Path(type_path) = &field.ty {
                if type_path
                    .path
                    .segments
                    .iter()
                    .any(|seg| seg.ident == "Option")
                {
                    return Some(field.ident.as_ref().unwrap());
                }
                return None;
            }
            None
        })
        .collect();
    let pri_field_str: Vec<_> = pri_field_names
        .iter()
        .map(|field| field.to_string())
        .collect();

    let expanded = quote! {
        #[async_trait::async_trait]
        impl Insertable for #struct_name {
            fn table_name() -> &'static str {
                #table_name
            }

            fn pri_column_names(&self) -> Vec<&'static str> {
                vec![#(#pri_field_str),*]
            }

            fn opt_column_names(&self) -> Vec<&'static str> {
                let mut cols = Vec::new();
                #(
                    if self.#opt_field_names.is_some() {
                        cols.push(stringify!(#opt_field_names));
                    }
                )*
                cols
            }

            fn bind_pri<'q>(&'q self, sql: &'q str) -> Query<'q, Postgres, PgArguments> {
                let mut query = sqlx::query(sql);
                #(query = query.bind(&self.#pri_field_names);)*
                query
            }

            fn bind_pri_to_query<'q>(
                &'q self,
                query: sqlx::query::Query<'q, sqlx::Postgres, PgArguments>,
            ) -> sqlx::query::Query<'q, sqlx::Postgres, PgArguments> {
                let mut query = query;
                #(query = query.bind(&self.#pri_field_names);)*
                query
            }

            fn bind_pri_to_query_as<'q, T>(
                &'q self,
                query: QueryAs<'q, Postgres, T, PgArguments>,
            ) -> QueryAs<'q, Postgres, T, PgArguments> {
                let mut query = query;
                #(query = query.bind(&self.#pri_field_names);)*
                query
            }

            fn bind_opt<'q>(&'q self, sql: &'q str) -> Query<'q, Postgres, PgArguments> {
                let mut query = sqlx::query(sql);
                #(
                    if self.#opt_field_names.is_some() {
                        query = query.bind(self.#opt_field_names.as_ref().unwrap().clone());
                    }
                )*
                query
            }

            fn bind_opt_to_query_as<'q, T>(
                &'q self,
                query: QueryAs<'q, Postgres, T, PgArguments>,
            ) -> QueryAs<'q, Postgres, T, PgArguments> {
                let mut query = query;
                #(
                    if self.#opt_field_names.is_some() {
                        query = query.bind(self.#opt_field_names.as_ref().unwrap().clone());
                    }
                )*
                query
            }

            fn bind_opt_to_query<'q>(
                &'q self,
                query: sqlx::query::Query<'q, sqlx::Postgres, PgArguments>,
            ) -> sqlx::query::Query<'q, sqlx::Postgres, PgArguments> {
                let mut query = query;
                #(
                    if self.#opt_field_names.is_some() {
                        query = query.bind(self.#opt_field_names.as_ref().unwrap().clone());
                    }
                )*
                query
            }

        }
    };

    TokenStream::from(expanded)
}
