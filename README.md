# [WIP] Omega

Named after Omega Centauri

Rust implementation of ZeroMQ Realtime Exchange Protocol (ZRE).

Architecture: https://rfc.zeromq.org/spec/36/

This library will be used as membership protocol for rust implementation of AnnaDB.

## Why ZRE for AnnaDB?

- It's a very simple yet powerful membership protocol thus the implementation is pretty easy and less error-prone.
- From my point of view, other probabilistic gossip schemes could be overkill for AnnaDB. As in most cases, it doesn't require the protocol to handle synchronized updates because of the monotonic nature of its core data structure lattice.
- It also provides some high-level APIs that hides the complexity of the networking library ZeroMQ. Hence, if required in future we can replace ZeroMQ with something else like [nanomsg](https://github.com/nanomsg/nanomsg).

## Goals of ZRE

The ZRE protocol provides a way for a set of nodes on a local network to discover each other, track when peers come and go, send messages to individual peers (unicast), and send messages to groups of peers (multicast).

- To work with no centralized services or mediation except those available by default on a network.
- To be robust on poor-quality networks, especially wireless networks.
- To minimize the time taken to detect a new peer’s arrival on the network.
- To recover from transient failures in connectivity.
- To be neutral with respect to operating system, programming language, and hardware.
- To allow any number of nodes to run in one process, to allow large-scale simulation and testing.

https://arxiv.org/pdf/1707.00788.pdf
https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf