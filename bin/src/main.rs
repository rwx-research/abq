use std::time::Duration;

use libabq::{
    protocol::{WorkAction, WorkId, WorkItem},
    queue::Abq,
};

fn main() {
    let mut queue = Abq::start();

    libabq::notify::send_work(
        queue.socket(),
        WorkItem {
            id: WorkId("i1".to_string()),
            action: WorkAction::Echo("hello".to_string()),
        },
    );
    libabq::notify::send_work(
        queue.socket(),
        WorkItem {
            id: WorkId("i2".to_string()),
            action: WorkAction::Echo("world".to_string()),
        },
    );
    let results = queue.shutdown_in(Duration::from_millis(100));

    dbg!(results);
}
