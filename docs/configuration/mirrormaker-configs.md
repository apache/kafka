---
title: MirrorMaker Configs
description: MirrorMaker Configs
weight: 9
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


Below is the configuration of the connectors that make up MirrorMaker 2. 

## MirrorMaker Common Configs

Below is the common configuration that applies to all three connectors. {{< include-html file="/static/42/generated/mirror_connector_config.html" >}} 

## MirrorMaker Source Configs

Below is the configuration of MirrorMaker 2 source connector for replicating topics. {{< include-html file="/static/42/generated/mirror_source_config.html" >}} 

## MirrorMaker Checkpoint Configs

Below is the configuration of MirrorMaker 2 checkpoint connector for emitting consumer offset checkpoints. {{< include-html file="/static/42/generated/mirror_checkpoint_config.html" >}} 

## MirrorMaker HeartBeat Configs

Below is the configuration of MirrorMaker 2 heartbeat connector for checking connectivity between connectors and clusters. {{< include-html file="/static/42/generated/mirror_heartbeat_config.html" >}} 
