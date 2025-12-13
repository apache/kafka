---
title: MirrorMaker Configs
description: MirrorMaker Configs
weight: 8
tags: ['kafka', 'docs']
aliases: 
keywords: 
type: docs
---

Below is the configuration of the connectors that make up MirrorMaker 2. 

## MirrorMaker Common Configs

Below are the common configuration properties that apply to all three connectors. {{< include-html file="/static/40/generated/mirror_connector_config.html" >}} 

## MirrorMaker Source Configs

Below is the configuration of MirrorMaker 2 source connector for replicating topics. {{< include-html file="/static/40/generated/mirror_source_config.html" >}} 

## MirrorMaker Checkpoint Configs

Below is the configuration of MirrorMaker 2 checkpoint connector for emitting consumer offset checkpoints. {{< include-html file="/static/40/generated/mirror_checkpoint_config.html" >}} 

## MirrorMaker HeartBeat Configs

Below is the configuration of MirrorMaker 2 heartbeat connector for checking connectivity between connectors and clusters. {{< include-html file="/static/40/generated/mirror_heartbeat_config.html" >}} 
