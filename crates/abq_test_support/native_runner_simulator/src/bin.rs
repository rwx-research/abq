use abq_native_runner_simulation::{read_msgs, run_simulation};

#[tokio::main]
async fn main() {
    let msgs_file = std::env::args().nth(1).unwrap();
    let msgs = std::fs::read_to_string(msgs_file).unwrap();
    let msgs = read_msgs(&msgs);
    run_simulation(msgs).await;
}
