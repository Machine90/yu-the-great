

#[cfg_attr(doc, aquamarine::aquamarine)]
/// Transporter of logic topology, it's maintain the basic connection  
/// between nodes of the topology
/// ### Diagram
/// ```mermaid 
/// flowchart LR
/// subgraph Transporter
/// subgraph pc1[PeerRpcClient]
/// 	cn1(current_node)
/// end
/// subgraph pc2[PeerRpcClient]
/// 	cn2(current_node)
/// end
/// subgraph gc1[GroupRpcClient]
/// 	cg1(current_group)
/// end
/// subgraph LTopo[Logic Topology]
/// subgraph Group1
/// 	subgraph ln1[Node1]
/// 		id1[NodeID]
/// 	end
/// 	subgraph ln2[Node2]
/// 		id2[NodeID]
/// 	end
/// end
/// subgraph Group2
/// 	ln3[Node3]
/// 	ln4[Node4]
/// end
/// end
/// cn1 -. find .-> ln1
/// cn2 -. find .-> ln2
/// cg1 -. find .-> Group2
/// end
/// subgraph PTopo[Physical Topology]
/// 	Node1
/// 	Node2
/// 	Node3
/// 	Node4
/// end
/// ln1== connect to ==>Node1
/// ln2==>Node2
/// ln3==>Node3
/// ln4==>Node4
/// ```
#[allow(unused)] struct TopoDesc;