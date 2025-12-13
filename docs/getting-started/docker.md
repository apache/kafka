---
title: Docker
description: 
weight: 8
tags: ['kafka', 'docs']
aliases: 
keywords: 
type: docs
---

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
