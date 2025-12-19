---
title: Kafka Configuration
description: Kafka Configuration
weight: 5
tags: ['kafka', 'docs']
aliases: 
keywords: 
type: docs
---

## Important Client Configurations

The most important producer configurations are: 

  * acks
  * compression
  * batch size

The most important consumer configuration is the fetch size. 

All configurations are documented in the configuration section. 

## A Production Server Config

Here is an example production server configuration: 
    
    
      # ZooKeeper
      zookeeper.connect=[list of ZooKeeper servers]
    
      # Log configuration
      num.partitions=8
      default.replication.factor=3
      log.dir=[List of directories. Kafka should have its own dedicated disk(s) or SSD(s).]
    
      # Other configurations
      broker.id=[An integer. Start with 0 and increment by 1 for each new broker.]
      listeners=[list of listeners]
      auto.create.topics.enable=false
      min.insync.replicas=2
      queued.max.requests=[number of concurrent requests]

Our client configuration varies a fair amount between different use cases. 
