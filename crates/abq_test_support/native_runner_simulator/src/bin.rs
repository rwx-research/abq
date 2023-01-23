use abq_native_runner_simulation::{read_msgs, run_simulation};

fn main() {
    let msgs = std::env::args().nth(1).unwrap();
    let msgs = read_msgs(&msgs);
    run_simulation(msgs);
}
