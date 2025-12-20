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


## JVM Based Apache Kafka Docker Image

[Docker](https://www.docker.com/) is a popular container runtime. Docker images for the JVM based Apache Kafka can be found on [Docker Hub](https://hub.docker.com/r/apache/kafka) and are available from version 3.7.0. 

Docker image can be pulled from Docker Hub using the following command: 
    
    
    $ docker pull apache/kafka:4.0.1

If you want to fetch the latest version of the Docker image use following command: 
    
    
    $ docker pull apache/kafka:latest

To start the Kafka container using this Docker image with default configs and on default port 9092: 
    
    
    $ docker run -p 9092:9092 apache/kafka:4.0.1

## GraalVM Based Native Apache Kafka Docker Image

Docker images for the GraalVM Based Native Apache Kafka can be found on [Docker Hub](https://hub.docker.com/r/apache/kafka-native) and are available from version 3.8.0.  
NOTE: This image is experimental and intended for local development and testing purposes only; it is not recommended for production use. 

Docker image can be pulled from Docker Hub using the following command: 
    
    
    $ docker pull apache/kafka-native:4.0.1

If you want to fetch the latest version of the Docker image use following command: 
    
    
    $ docker pull apache/kafka-native:latest

To start the Kafka container using this Docker image with default configs and on default port 9092: 
    
    
    $ docker run -p 9092:9092 apache/kafka-native:4.0.1

## Usage guide

Detailed instructions for using the Docker image are mentioned [here](https://github.com/apache/kafka/blob/trunk/docker/examples/README.md). 
