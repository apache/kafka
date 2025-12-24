---
title: Tiered Storage Configs
description: Tiered Storage Configs
weight: 11
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


Below is the Tiered Storage configuration. {{< include-html file="/static/43/generated/remote_log_manager_config.html" >}} 

## RLMM Configs

Below is the configuration for `TopicBasedRemoteLogMetadataManager`, which is the default implementation of `RemoteLogMetadataManager`.

All configurations here should start with the prefix defined by `remote.log.metadata.manager.impl.prefix`, for example, `rlmm.config.remote.log.metadata.consume.wait.ms`.

{{< include-html file="/static/43/generated/remote_log_metadata_manager_config.html" >}} 

The implementation of `TopicBasedRemoteLogMetadataManager` needs to create admin, producer, and consumer clients for the internal topic `__remote_log_metadata`.

Additional configurations can be provided for different types of clients using the following configuration properties: 
    
    
    # Configs for admin, producer, and consumer clients
    <rlmm.prefix>.remote.log.metadata.common.client.<kafka.property> = <value>
    
    # Configs only for producer client
    <rlmm.prefix>.remote.log.metadata.producer.<kafka.property> = <value>
    
    # Configs only for consumer client
    <rlmm.prefix>.remote.log.metadata.consumer.<kafka.property> = <value>
