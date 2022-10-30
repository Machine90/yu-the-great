### Backlogs

| Features                             | Effect modules                                              | Details                                                      | Status |
| ------------------------------------ | ----------------------------------------------------------- | ------------------------------------------------------------ | ------ |
| optimize `read_index`  in async mode | consensus.raft.read_only<br />application.node.process.read | Optimize `read_index` procedure and handle read in parallel, which leader receive `read_index` request, make the raft step and handle it async, current thread just waiting for read_state ready and notified. And all "read request" should be advanced and take out for processing while leader receiving broadcast responses from followers. For example leader receive "read request" [1,2,3,4,5] and all read_ctx has been broadcast to follower, but response of request 4 is arrived (and reached majority) first. then 1,2,3,4 should be take out for reading, and all these read results should be notified. | done |
|                                      |                                                             |                                                              |        |
|                                      |                                                             |                                                              |        |
|                                      |                                                             |                                                              |        |

