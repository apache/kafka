---
title: Kafka Configuration
description: Kafka Configuration
weight: 5
tags: ['kafka', 'docs']
aliases: 
keywords: 
type: docs
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->


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
