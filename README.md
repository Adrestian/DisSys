# MIT 6.824 Distributed Systems Lab Assignments
**Note: Building Plugin is not supported on Windows**
## [Lab 1: Mini MapReduce](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)
- **Done**
- Test: 
    1. `cd src/main`
    2. `bash test-mr.sh`
### Future Tasks/Improvement: 
- when the job is about to end, the idling worker will use RPC busily to ask for work and get nothing back, there could be some optimizations here(as described by the paper)
- write a distributed grep 
## [Lab 2: Raft](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html)
- In Progress
- Resources:
    - [Raft Visualized](http://thesecretlivesofdata.com/raft/)
    - [Students' Guide to Raft](https://thesquareplanet.com/blog/students-guide-to-raft/)
    - [Instructors' Guide to Raft](https://thesquareplanet.com/blog/instructors-guide-to-raft/)
    - [Raft Locking Advice](https://pdos.csail.mit.edu/6.824/labs/raft-locking.txt)
    - [Raft Structure Advice](https://pdos.csail.mit.edu/6.824/labs/raft-structure.txt)
    - Make reading a bit easier: `document.querySelector('body').style.fontFamily = "Consolas"`

## Lab 3: KV Raft
- In Progress

## Lab 4: Sharded KV
- In Progress