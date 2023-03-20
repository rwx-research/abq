use proc_macro::TokenStream;
use quote::quote;
use syn::Lit;

/// Proc macro that runs a body N times.
#[proc_macro_attribute]
pub fn n_times(args: TokenStream, item: TokenStream) -> TokenStream {
    let fun = syn::parse_macro_input!(item as syn::ItemFn);

    let n = syn::parse_macro_input!(args as Lit);
    let n: usize = match n {
        Lit::Int(n) => n.base10_parse().unwrap(),
        _ => panic!("N must be an integer"),
    };

    let name = fun.sig.ident.clone();
    let args = fun.sig.inputs.clone();
    let body = fun.block.clone();
    let asyncness = fun.sig.asyncness;
    let visibility = &fun.vis;
    let attributes = fun.attrs;

    let new_fn = quote! {
        #(#attributes)*
        #visibility #asyncness fn #name(#args) {
            for i in 0..#n {
                #body
            }
        }
    };

    new_fn.into()
}
