<img src="./imgs/logo.png" width="300"/>

**yu-the-great** 是一个由 Rust 语言开发的基于 Raft 算法实现的分布式共识库，开发者可以使用它来进行二次开发，可以更加简单的去开发一些基于 Raft 共识的分布式产品，而无需去考虑 Raft 本身的一些细节。比如基于 yu (以下 yu-the-great 我们简称 yu) 去开发一个单共识组 key-value 存储的配置中心、微服务注册中心，可以开发一个强一致的分布式锁，甚至是基于 Multi-Raft 架构开发一个可扩容的分布式 kv 数据库。任何能想到的具有 CAP 的分布式场景，yu 都可以协助实现。

yu 在使用的方式上表现为一个 rust 库，开发者无需像服务端进程一样单独为它进行部署，只需要把它引入您的项目中，就可以简单的使用起来，而业务代码则是作为 yu 的 listener 来感知集群的事件，例如集群提交 propose, 集群读请求处理，集群软状态 (softstate) 变化 (leader 发生改变、当前节点角色发生改变等). 本项目受到一些优秀的分布式项目的启发，例如 Multi-Raft 的架构和协处理器的设计上受到 HBase, TiKV 的启发，Raft 算法则来源于 etcd-raft 以及 raft-rs 项目的启发。

关于名字的由来，"yu-the-great" 大禹，起名并非随意，而是在 2021 年立项之初，初衷是想做一个帮助大家更好更简单的治理、确保数据一致性的库，大禹是华夏神话里治理洪水的英雄，他治理洪水的方法不是去堵截，而是靠去疏通、引导，让洪水自然流向大海，我们的项目则是致力于将网络的 “洪流” 有序且正确的被引导到它该去的地方 (例如业务持久化的存储)。



## 快速开始

### 依赖引入

目前我们支持利用 Rust Cargo 本身所支持的 git 路径的方式引入，因为暂时还没有将依赖发布到一些其他的仓库例如 crates.io (暂时还在计划中)


```rust
[dependencies]
# full features: ["multi", "single", "rpc"]
yu-the-great = { git = "https://github.com/Machine90/yu-the-great.git", default-features = false }
```



### 代码示例

#### HelloWorld

```rust
// 引入时选择特性(单 raft 组，采用 tarpc 通信) features = ["single", "rpc"]
use std::{io::Result};
use yu_the_great as yu;
use yu::{
    common::protocol::{read_state::ReadState,},
    coprocessor::listener::{proposal::RaftListener, Acl, RaftContext},
    peer::{config::NodeConfig, facade::Facade},
    protos::{raft_group_proto::GroupProto},
    solutions::builder::single::{provider, Builder},
    solutions::rpc::StubConfig,
    torrent::{topology::node::Node},
    RaftRole
};

/// 第一步: 定义一个 struct 用于打印 "hello" 等信息
struct HelloWorld;
/// 默认开启访问的 (如果需要对某些组或者别的维度的数据进行限制，可以实现其方法)
impl Acl for HelloWorld {}
/// 第二步: 给 HelloWorld 实现一个 `RaftListener` 用以接收集群的读、写请求及其他事件
#[yu::async_trait]
impl RaftListener for HelloWorld {
    
    /// propose 发送至集群，由 Leader 处理，edit_log 会被作为 RaftEntry Append 至 RaftLog, 
    /// 一旦 leader 状态机 applied 至当前 commit, 此接口回调将被触发，具体业务逻辑交由使用者进行
    /// 实现，Raft 本身只关心集群 CAP.
    async fn handle_write(&self, ctx: &RaftContext, edit_log: &[u8]) -> Result<i64> {
        let RaftContext { node_id, .. } = ctx;
        println!("hello {:?} this is Node-{}", String::from_utf8(edit_log.to_vec()), node_id);
        Ok(edit_log.len() as i64)
    }

    /// yu 支持 quorum read 一致性读, 其实现同 etcd-raft, 即论文中提及的 ReadOnly 实现，读请求由
    /// Leader 处理，读请求处理时，具体读的逻辑调用此接口，读请求上下文将传递至 `ReadState` 之中。读请求
    /// 默认使用 SafeRead, 即读处理前 Leader 触发心跳确认是否仍为 Leader
    async fn handle_read(&self, _: &RaftContext, _: &mut ReadState) {
        // ignore this.
    }
}

// 这里我们用三个线程模拟三个节点的单 Raft 集群
fn main() {
    let group = GroupProto {
        id: 1, // 当前为组 1
        confstate: Some(vec![1, 2, 3].into()), // 该组由 [1,2,3] 三个成员组成
        // 三个成员分别的物理地址
        endpoints: vec![
            Node::parse(1, "raft://localhost:8081").unwrap().into(),
            Node::parse(2, "raft://localhost:8082").unwrap().into(),
            Node::parse(3, "raft://localhost:8083").unwrap().into(),
        ],
        ..Default::default()
    };
    let mut peers = vec![];
    
    // 第三部: 我们为当前应用创建 Raft 节点
    for node_id in 1..=3 {
        let mut conf = NodeConfig::default();
        // 节点 id 必须要指定，标识唯一的节点
        conf.id = node_id;
        let node = Builder::new(group.clone(), conf)
            .with_raftlog_store(|group| provider::mem_raftlog_store(group)) // 存储 log entry 到内存
            .use_default() // 默认采用 RPC 通信，并自动更新 raft tick.
            .add_raft_listener(HelloWorld(node_id)) // 把实现了 listener 的结构注册到此 Raft 节点
            .build() // 完成了
            .unwrap();
        // 我们可以将此节点运行在一个守护线程里
        let _ = node.start_rpc(StubConfig {
            address: format!("localhost:808{node_id}"), // RPC server's address
            run_as_daemon: true, // 作为一个存根服务器来接收 raft 信息
            print_banner: false,
        });
        let peer = node.get_local_client();
        peers.push(peer);
    }
    // 接着我们立即选出一个 leader 节点.
    for peer in peers.iter() {
        if let Ok(RaftRole::Leader) = peer.election() {
            break;
        }
    }
    // 最后对每个节点我们分别发送一条“提议” (propose), propose 会在指定超时时间内等待本次提议“写”成功
    // 若超时则会返回 Pending，但这并不意味着写失败，只是当前集群在超时时间内尚未收到大多数节点相应 Append
    for (i, name) in ["Alice", "Bob", "Charlie"].iter().enumerate() {
        let proposal = peers[i].propose_bin(name);
        assert!(proposal.is_ok());
    }
}
```



#### 更多示例

请移步: https://github.com/Machine90/yu-examples

