---
title: Docker
description: 
weight: 6
tags: ['kafka', 'docs']
aliases: 
keywords: 
type: docs
---

## JVM Based Apache Kafka Docker Image

[Docker](https://www.docker.com/) is a popular container runtime. Docker images for the JVM based Apache Kafka can be found on [Docker Hub](https://hub.docker.com/r/apache/kafka) and are available from version 3.7.0. 

Docker image can be pulled from Docker Hub using the following command: 
    
    
    $ docker pull apache/kafka:4.0.0

If you want to fetch the latest version of the Docker image use following command: 
    
    
    $ docker pull apache/kafka:latest

To start the Kafka container using this Docker image with default configs and on default port 9092: 
    
    
    $ docker run -p 9092:9092 apache/kafka:4.0.0

## GraalVM Based Native Apache Kafka Docker Image

Docker images for the GraalVM Based Native Apache Kafka can be found on [Docker Hub](https://hub.docker.com/r/apache/kafka-native) and are available from version 3.8.0.  
NOTE: This image is experimental and intended for local development and testing purposes only; it is not recommended for production use. 

Docker image can be pulled from Docker Hub using the following command: 
    
    
    $ docker pull apache/kafka-native:4.0.0

If you want to fetch the latest version of the Docker image use following command: 
    
    
    $ docker pull apache/kafka-native:latest

To start the Kafka container using this Docker image with default configs and on default port 9092: 
    
    
    $ docker run -p 9092:9092 apache/kafka-native:4.0.0

## Usage guide

Detailed instructions for using the Docker image are mentioned [here](https://github.com/apache/kafka/blob/trunk/docker/examples/README.md). 
