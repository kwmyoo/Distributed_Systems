# Distributed_Systems
 
1. RPC

Implementation of Remote Procedure Call. The input file operation is given as C code. 
A middle proxy layer using cache takes client's request, parses it to Java code and handles the request.
Caching is implemented in the proxy using on-demand caching, check-on-use and LRU eviction policy which obeys session semantics.
The server (origin of the files) is also implemented to keep track of updates from different cache sites.

2. Elastic server

In a simulated environment of online shopping mall, the code is supposed to elastically scale the number of servers according to 
the current client load. The servers are split into three tiers: front, back and cache for DB. Each tier scales independently,
while the queue length (of requests) for each server is used as scaling signal.

3. Two-Phase Commit

In a simulated environment of photos-gathering app, for each request of collage (photos) commit, the server undergoes a
two-phase commit process that controls the consistency of files across different network nodes.
