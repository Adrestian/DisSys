# MIT 6.824 Distributed Systems Labs
## [Lab 1: MapReduce](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)
- Test: 
    1. `cd src/main`
    2. `bash test-mr.sh`
### Todo: 
- Duplicate work to multiple workers to reduce "tail" latency
- Make a distributed `grep`
## [Lab 2: Raft](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html)
- Test All: `for i in {0..10}; do time go test; done;`
### Lab 2A: Leader Election
- Test:
    1. `cd src/raft`
    2. `go test -run 2A -race`
### Lab 2B: Log Replication
- Test:
    1. `go test -run 2B -race` 
### Lab 2C: Persistence
- Test:
    1. `go test -run 2C -race`
### Lab 2D: Log Compaction
- *In Progress...*

### Additional Resources
- [Raft Visualized](http://thesecretlivesofdata.com/raft/)
- [Students' Guide to Raft](https://thesquareplanet.com/blog/students-guide-to-raft/)
- [Instructors' Guide to Raft](https://thesquareplanet.com/blog/instructors-guide-to-raft/)
- [Raft Locking Advice](https://pdos.csail.mit.edu/6.824/labs/raft-locking.txt)
- [Raft Structure Advice](https://pdos.csail.mit.edu/6.824/labs/raft-structure.txt)
- [John Ousterhout, UIUC lecture: Designing for Understandability: The Raft Consensus Algorithm](https://www.youtube.com/watch?v=vYp4LYbnnW8)
- Make reading a bit easier: `$('body').style.fontFamily = "Consolas"`

## Lab 3: KV Raft
- Idle

## Lab 4: Sharded KV
- Idle
