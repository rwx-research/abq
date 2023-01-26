use proc_macro::TokenStream;
use quote::quote;
use syn::{Lit, NestedMeta};

/// Proc macro that runs a body N times.
#[proc_macro_attribute]
pub fn n_times(args: TokenStream, item: TokenStream) -> TokenStream {
    let fun = syn::parse_macro_input!(item as syn::ItemFn);

    let mut args = syn::parse_macro_input!(args as syn::AttributeArgs);
    if args.len() != 1 {
        panic!("must have exactly one argument, N");
    }
    let n = args.pop().unwrap();
    let n: usize = match n {
        NestedMeta::Lit(Lit::Int(n)) => n.base10_parse().unwrap(),
        _ => panic!("N must be an integer"),
    };

    let name = fun.sig.ident.clone();
    let args = fun.sig.inputs.clone();
    let body = fun.block.clone();
    let visibility = &fun.vis;
    let attributes = fun.attrs;

    let new_fn = quote! {
        #(#attributes)*
        #visibility fn #name(#args) {
            for _ in 0..#n {
                #body
            }
        }
    };

    new_fn.into()
}
