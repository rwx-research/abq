use std::time::Duration;

use libabq::protocol::{WorkAction, WorkId};
use libabq::queue::Abq;

fn main() {
    libabq::queue::init();

    let mut queue = Abq::start();

    libabq::notify::send_work(
        queue.socket(),
        WorkId("i1".to_string()),
        WorkAction::Echo("hello".to_string()),
    );
    libabq::notify::send_work(
        queue.socket(),
        WorkId("i2".to_string()),
        WorkAction::Echo("world".to_string()),
    );
    let results = queue.shutdown_in(Duration::from_millis(100));

    dbg!(results);
}
