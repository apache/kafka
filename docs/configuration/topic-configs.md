---
title: Topic Configs
description: Topic Configs
weight: 2
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


Configurations pertinent to topics have both a server default as well an optional per-topic override. If no per-topic configuration is given the server default is used. The override can be set at topic creation time by giving one or more `--config` options. This example creates a topic named _my-topic_ with a custom max message size and flush rate: 
    
    
    $ bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic my-topic --partitions 1 \
      --replication-factor 1 --config max.message.bytes=64000 --config flush.messages=1

Overrides can also be changed or set later using the alter configs command. This example updates the max message size for _my-topic_ : 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name my-topic
      --alter --add-config max.message.bytes=128000

To check overrides set on the topic you can do 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name my-topic --describe

To remove an override you can do 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092  --entity-type topics --entity-name my-topic
      --alter --delete-config max.message.bytes

Below is the topic configuration. The server's default configuration for this property is given under the Server Default Property heading. A given server default config value only applies to a topic if it does not have an explicit topic config override. {{< include-html file="/static/41/generated/topic_config.html" >}} 
