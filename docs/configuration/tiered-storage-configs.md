---
title: Tiered Storage Configs
description: Tiered Storage Configs
weight: 11
tags: ['kafka', 'docs']
aliases: 
keywords: 
type: docs
---

Below is the Tiered Storage configuration. {{< include-html file="/static/42/generated/remote_log_manager_config.html" >}} 

## RLMM Configs

Below is the configuration for `TopicBasedRemoteLogMetadataManager`, which is the default implementation of `RemoteLogMetadataManager`.

All configurations here should start with the prefix defined by `remote.log.metadata.manager.impl.prefix`, for example, `rlmm.config.remote.log.metadata.consume.wait.ms`.

{{< include-html file="/static/42/generated/remote_log_metadata_manager_config.html" >}} 

The implementation of `TopicBasedRemoteLogMetadataManager` needs to create admin, producer, and consumer clients for the internal topic `__remote_log_metadata`.

Additional configurations can be provided for different types of clients using the following configuration properties: 
    
    
    # Configs for admin, producer, and consumer clients
    <rlmm.prefix>.remote.log.metadata.common.client.<kafka.property> = <value>
    
    # Configs only for producer client
    <rlmm.prefix>.remote.log.metadata.producer.<kafka.property> = <value>
    
    # Configs only for consumer client
    <rlmm.prefix>.remote.log.metadata.consumer.<kafka.property> = <value>
