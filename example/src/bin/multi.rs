use components::mailbox::multi::api::MultiRaftApi;
use components::mailbox::multi::model::Located;
use components::tokio1::runtime::Runtime;
use components::tokio1::time::{sleep, Instant};
use components::{torrent::runtime, vendor::prelude::LogLevel};
use multi_db::node_three;
use multi_db::set_logger;
use std::hint::spin_loop;
use std::sync::Arc;
use std::time::Duration;

use crate::multi_db::kv::Operation;

mod multi_db;

fn main() {
    set_logger(LogLevel::Info, true);
    Runtime::new().unwrap().block_on(three());
}

#[allow(unused)]
async fn three() {
    let mut ns = vec![];
    for id in 1..=3 {
        let n = node_three::build_node(id, format!("127.0.0.1:2007{id}").as_str());
        ns.push(n);
    }

    sleep(Duration::from_millis(18000)).await;
    // node 1 group 1
    let nodes = Arc::new(ns);
    let mut tasks = vec![];

    // for word in ["asdas", "ggqwwq", "pwqieo", "ffff", "zawd"] {
    for word in ["1", "101", "111", "777", "zawd", "222"] {
        // let word = "aaaaa"; // make propose to same group.
        let ns = nodes.clone();
        let task = runtime::spawn(async move {
            let p = ns[0].propose(Operation::put(word, word)).await;
            println!("put key: {word} => {:?}", p)
        });
        tasks.push(task);
    }

    let time = Instant::now();
    for task in tasks {
        let _ = task.await;
    }
    println!("elapsed: {:?}ms", time.elapsed().as_millis());
    let read = nodes[0].read_index(Operation::get("111")).await;
    println!("readed {:?}", read);

    let p = nodes[0].read_index(Operation::get("dummy")).await;
    if let Ok(Located { result, .. }) = p {
        println!("{:?}", String::from_utf8(result.request_ctx));
    }
    let total = nodes[0].partitions().current().len();
    println!("{total} partitions");

    for (cnt, (_, p)) in nodes[0].partitions().current().iter().enumerate() {
        println!("{p} => {}", p.resident);
        if cnt > 16 {
            break;
        }
    }

    loop {
        spin_loop();
    }
}
