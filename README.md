# MIT 6.824 Distributed Systems Labs
## [Lab 1: Mini MapReduce](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)
- **Finished**
- Test: 
    1. `cd src/main`
    2. `bash test-mr.sh`
### Future Tasks/Improvement: 
- when the Map/Reduce phase is about to end, the idling worker will busily ask for work and get nothing back, there could be some optimizations here
    1. Coordinator duplicate work if the phase is about to end, as described by the paper or 
    2. Implement Exponential Backoff?
- write a distributed `grep`
## [Lab 2: Raft](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html)
### Lab 2A: Leader Election
- **Finished**
- Test:
    1. `cd src/raft`
    2. `go test -run 2A -race`


### Lab 2B: Log
- Idle
### Lab 2C: Persistence
- Idle
### Lab 2D: Log Compaction
- Idle

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