use proc_macro::TokenStream;
use quote::quote;

/// Proc macro that runs a body with very [protocol version][ProtocolWitness],
/// which is injected as a variable `proto`.
#[proc_macro_attribute]
pub fn with_protocol_version(_args: TokenStream, item: TokenStream) -> TokenStream {
    let fun = syn::parse_macro_input!(item as syn::ItemFn);

    let name = fun.sig.ident.clone();
    let args = fun.sig.inputs.clone();
    let body = fun.block.clone();
    let visibility = &fun.vis;
    let attributes = fun.attrs;

    let new_fn = quote! {
        #(#attributes)*
        #visibility fn #name(#args) {
            use abq_utils::net_protocol::runners::ProtocolWitness;
            for proto in ProtocolWitness::iter_all() {
                #body
            }
        }
    };

    new_fn.into()
}
