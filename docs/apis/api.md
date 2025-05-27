---
title: API
description: 
weight: 1
tags: ['kafka', 'docs']
aliases: 
keywords: 
type: docs
---

Kafka includes five core apis: 

  1. The Producer API allows applications to send streams of data to topics in the Kafka cluster. 
  2. The Consumer API allows applications to read streams of data from topics in the Kafka cluster. 
  3. The Streams API allows transforming streams of data from input topics to output topics. 
  4. The Connect API allows implementing connectors that continually pull from some source system or application into Kafka or push from Kafka into some sink system or application. 
  5. The Admin API allows managing and inspecting topics, brokers, and other Kafka objects. 
Kafka exposes all its functionality over a language independent protocol which has clients available in many programming languages. However only the Java clients are maintained as part of the main Kafka project, the others are available as independent open source projects. A list of non-Java clients is available [here](https://cwiki.apache.org/confluence/display/KAFKA/Clients). 

# Producer API

The Producer API allows applications to send streams of data to topics in the Kafka cluster. 

Examples showing how to use the producer are given in the [javadocs](/40/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html "Kafka 4.0 Javadoc"). 

To use the producer, you can use the following maven dependency: 
    
    
    <dependency>
    	<groupId>org.apache.kafka</groupId>
    	<artifactId>kafka-clients</artifactId>
    	<version>4.0.0</version>
    </dependency>

# Consumer API

The Consumer API allows applications to read streams of data from topics in the Kafka cluster. 

Examples showing how to use the consumer are given in the [javadocs](/40/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html "Kafka 4.0 Javadoc"). 

To use the consumer, you can use the following maven dependency: 
    
    
    <dependency>
    	<groupId>org.apache.kafka</groupId>
    	<artifactId>kafka-clients</artifactId>
    	<version>4.0.0</version>
    </dependency>

# Streams API

The [Streams](/40/streams) API allows transforming streams of data from input topics to output topics. 

Examples showing how to use this library are given in the [javadocs](/40/javadoc/index.html?org/apache/kafka/streams/KafkaStreams.html "Kafka 4.0 Javadoc"). 

Additional documentation on using the Streams API is available [here](/40/streams). 

To use Kafka Streams you can use the following maven dependency: 
    
    
    <dependency>
    	<groupId>org.apache.kafka</groupId>
    	<artifactId>kafka-streams</artifactId>
    	<version>4.0.0</version>
    </dependency>

When using Scala you may optionally include the `kafka-streams-scala` library. Additional documentation on using the Kafka Streams DSL for Scala is available [in the developer guide](/40/streams/developer-guide/dsl-api.html#scala-dsl). 

To use Kafka Streams DSL for Scala 2.13 you can use the following maven dependency: 
    
    
    <dependency>
    	<groupId>org.apache.kafka</groupId>
    	<artifactId>kafka-streams-scala_2.13</artifactId>
    	<version>4.0.0</version>
    </dependency>

# Connect API

The Connect API allows implementing connectors that continually pull from some source data system into Kafka or push from Kafka into some sink data system. 

Many users of Connect won't need to use this API directly, though, they can use pre-built connectors without needing to write any code. Additional information on using Connect is available [here](/documentation.html#connect). 

Those who want to implement custom connectors can see the [javadoc](/40/javadoc/index.html?org/apache/kafka/connect "Kafka 4.0 Javadoc"). 

# Admin API

The Admin API supports managing and inspecting topics, brokers, acls, and other Kafka objects. 

To use the Admin API, add the following Maven dependency: 
    
    
    <dependency>
    	<groupId>org.apache.kafka</groupId>
    	<artifactId>kafka-clients</artifactId>
    	<version>4.0.0</version>
    </dependency>

For more information about the Admin APIs, see the [javadoc](/40/javadoc/index.html?org/apache/kafka/clients/admin/Admin.html "Kafka 4.0 Javadoc"). 
