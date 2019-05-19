# Distributed_Systems
 
1. RPC

Implementation of Remote Procedure Call. The input file operation is given as C code. 
A middle proxy layer using cache takes client's request, parses it to Java code and handles the request.
Caching is implemented in the proxy using on-demand caching, check-on-use and LRU eviction policy which obeys session semantics.
The server (origin of the files) is also implemented to keep track of updates from different cache sites.
