
#[cfg_attr(doc, aquamarine::aquamarine)]
/// [Node](application::multi::node::Node)
/// ```mermaid
/// flowchart LR
/// subgraph Node1
///     p1[Peer_1,1\ngroup: 1, node: 1]
///     p2[Peer_2,1\ngroup: 2, node: 1]
///     p3[Peer_3,1\ngroup: 3, node: 1]
/// end
/// subgraph Node2
///     p4[Peer_1,2\ngroup: 1, node: 2]
///     p5[Peer_2,2\ngroup: 2, node: 2]
///     p6[Peer_4,2\ngroup: 4, node: 2]
/// end
/// subgraph Node3
///     p7[Peer_1,3\ngroup: 1, node: 3]
///     p8[Peer_3,3\ngroup: 3, node: 3]
///     p9[Peer_4,3\ngroup: 4, node: 3]
/// end
/// subgraph Node4
///     p10[Peer_2,4\ngroup: 2, node: 4]
///     p11[Peer_3,4\ngroup: 3, node: 4]
///     p12[Peer_4,4\ngroup: 4, node: 4]
/// end
/// p1-->p4-->p7
/// p2-->p5-->p10
/// p3-->p8-->p11
/// p6-->p9-->p12
/// ```
#[allow(unused)] struct NodeDesc;
