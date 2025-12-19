---
title: Security Overview
description: Security Overview
weight: 1
tags: ['kafka', 'docs']
aliases: 
keywords: 
type: docs
---

In release 0.9.0.0, the Kafka community added a number of features that, used either separately or together, increases security in a Kafka cluster. The following security measures are currently supported: 

  1. Authentication of connections to brokers from clients (producers and consumers), other brokers and tools, using either SSL or SASL. Kafka supports the following SASL mechanisms: 
     * SASL/GSSAPI (Kerberos) - starting at version 0.9.0.0
     * SASL/PLAIN - starting at version 0.10.0.0
     * SASL/SCRAM-SHA-256 and SASL/SCRAM-SHA-512 - starting at version 0.10.2.0
     * SASL/OAUTHBEARER - starting at version 2.0
  2. Authentication of connections from brokers to ZooKeeper
  3. Encryption of data transferred between brokers and clients, between brokers, or between brokers and tools using SSL (Note that there is a performance degradation when SSL is enabled, the magnitude of which depends on the CPU type and the JVM implementation.)
  4. Authorization of read / write operations by clients
  5. Authorization is pluggable and integration with external authorization services is supported

It's worth noting that security is optional - non-secured clusters are supported, as well as a mix of authenticated, unauthenticated, encrypted and non-encrypted clients. The guides below explain how to configure and use the security features in both clients and brokers. 
