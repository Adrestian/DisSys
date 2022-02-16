# MIT 6.824 Distributed Systems Labs
## [Lab 1: Mini MapReduce](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)
- **Finished**
- Test: 
    1. `cd src/main`
    2. `bash test-mr.sh`
### Future Tasks/Improvement: 
- When the Map/Reduce phase is about to end, the idling worker will busily ask for work and get nothing back, there could be some optimizations here
    1. Coordinator should duplicate work if the phase is coming to an end, as described by the paper or 
    2. Implement Exponential Backoff?
- Make a distributed `grep`
## [Lab 2: Raft](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html)
### Lab 2A: Leader Election
- **Finished**
- Test:
    1. `cd src/raft`
    2. `for i in {0..10}; do time go test -run 2A -race; done`
### Lab 2B: Log
- *Finished*
- Test:
    1. `go test -run 2B -race` 
### Lab 2C: Persistence
- *Finished*
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
- Make reading a bit easier: `document.querySelector('body').style.fontFamily = "Consolas"`

## Lab 3: KV Raft
- Idle

## Lab 4: Sharded KV
- Idle
