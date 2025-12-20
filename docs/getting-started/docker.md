---
title: Docker
description: 
weight: 8
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


## Introduction

[Docker](https://www.docker.com/) is a popular container runtime. Docker images for Apache Kafka can be found on [Docker Hub](https://hub.docker.com/r/apache/kafka) and are available from version 3.7.0. 

## Getting the kafka docker image

Docker image can be pulled from Docker Hub using the following command:- 
    
    
    $ docker pull apache/kafka:3.7.2

If you want to fetch the latest version of the docker image use following command:- 
    
    
    $ docker pull apache/kafka:latest

## Start kafka with default configs

Run docker image on default port 9092:- 
    
    
    $ docker run -p 9092:9092 apache/kafka:3.7.2

## Usage guide

Detailed instructions for using the docker image are mentioned [here](https://github.com/apache/kafka/blob/trunk/docker/examples/README.md). 
