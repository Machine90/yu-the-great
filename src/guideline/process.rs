
#[cfg_attr(doc, aquamarine::aquamarine)]
/// Transfer of state see:
/// ```mermaid
/// stateDiagram-v2
/// state Leader {
/// 	lsa: send_append
/// }
/// state Follower {
/// 	hs: handle_snapshot(Follower)
/// 	hae: handle_append_entries(Follower)
/// }
/// lsa--> hs: Send Snapshot Message
/// lsa --> hae: Send Append Message
/// hs --> handle_append_response: Accept
/// hae --> handle_append_response: Accept
/// hae --> handle_append_response: Reject
/// state handle_append_response {
/// 	[*] --> reject: Reject Append
/// 	[*] --> next_state: Accept Append
/// 	reject --> send_append: not replicated
/// 	reject --> enter_probe: replicate before
/// 	enter_probe --> send_append
/// 	next_state --> send_append
/// 	send_append --> [*]
/// 	state send_append {
/// 		sa1: Snapshot
/// 		sa2: Snapshot
/// 		pa1: Probe or Replicate
/// 		[*] --> sa2: If state is
/// 		[*] --> pa1: If state is
/// 		pa1 --> sa1: 1. still pending request snap\n2. not matched first index\nthen set pending_snapshot
/// 		pa1 --> [*]: Send Append Message
/// 		sa1 --> [*]: Send Snapshot Message
/// 		sa2 --> [*]
/// 	}
/// 	state enter_probe {
/// 		pe1: Probe
/// 		re1: Replicate
/// 		[*] --> re1
/// 		re1 --> pe1
/// 		pe1 --> [*]
/// 	}
/// 	state next_state {
/// 		s1: Snapshot
/// 		p1: Probe
/// 		r1: Replicate
/// 		p2: Probe
/// 		r2: Replicate
/// 		[*] --> s1
/// 		[*] --> p2
/// 		[*] --> r1
///         s1 --> p1: snapshot abort
///         s1 --> [*]: pending snapshot
///         p2 --> r2
///         p1 --> [*]
///         r1 --> [*]
///         r2 --> [*]
///     }
/// }
/// ```
#[allow(unused)] struct ProgressStateDesc;