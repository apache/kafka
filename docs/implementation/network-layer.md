---
title: Network Layer
description: Network Layer
weight: 1
tags: ['kafka', 'docs']
aliases: 
keywords: 
type: docs
---

# Network Layer

The network layer is a fairly straight-forward NIO server, and will not be described in great detail. The sendfile implementation is done by giving the `TransferableRecords` interface a `writeTo` method. This allows the file-backed message set to use the more efficient `transferTo` implementation instead of an in-process buffered write. The threading model is a single acceptor thread and _N_ processor threads which handle a fixed number of connections each. This design has been pretty thoroughly tested [elsewhere](https://web.archive.org/web/20120619234320/https://sna-projects.com/blog/2009/08/introducing-the-nio-socketserver-implementation/) and found to be simple to implement and fast. The protocol is kept quite simple to allow for future implementation of clients in other languages. 
