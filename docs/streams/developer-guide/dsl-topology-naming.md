---
title: Naming Operators in a Streams DSL application
description: 
weight: 5
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


# Developer Guide for Kafka Streams

# Naming Operators in a Kafka Streams DSL Application

You now can give names to processors when using the Kafka Streams DSL. In the PAPI there are `Processors` and `State Stores` and you are required to explicitly name each one. 

At the DSL layer, there are operators. A single DSL operator may compile down to multiple `Processors` and `State Stores`, and if required `repartition topics`. But with the Kafka Streams DSL, all these names are generated for you. There is a relationship between the generated processor name state store names (hence changelog topic names) and repartition topic names. Note, that the names of state stores and changelog/repartition topics are "stateful" while processor names are "stateless". 

This distinction of stateful vs. stateless names has important implications when updating your topology. While the internal naming makes creating a topology with the DSL much more straightforward, there are a couple of trade-offs. The first trade-off is what we could consider a readability issue. The other more severe trade-off is the shifting of names due to the relationship between the DSL operator and the generated `Processors`, `State Stores` changelog topics and repartition topics. 

# Readability Issues

By saying there is a readability trade-off, we are referring to viewing a description of the topology. When you render the string description of your topology via the `Topology#describe()` method, you can see what the processor is, but you don't have any context for its business purpose. For example, consider the following simple topology:   

    
    
    KStream<String,String> stream = builder.stream("input");
    stream.filter((k,v) -> !v.equals("invalid_txn"))
    	  .mapValues((v) -> v.substring(0,5))
    	  .to("output");

Running `Topology#describe()` yields this string: 
    
    
    Topologies:
       Sub-topology: 0
    	Source: KSTREAM-SOURCE-0000000000 (topics: [input])
    	  --> KSTREAM-FILTER-0000000001
    	Processor: KSTREAM-FILTER-0000000001 (stores: [])
    	  --> KSTREAM-MAPVALUES-0000000002
    	  <-- KSTREAM-SOURCE-0000000000
    	Processor: KSTREAM-MAPVALUES-0000000002 (stores: [])
    	  --> KSTREAM-SINK-0000000003
    	  <-- KSTREAM-FILTER-0000000001
    	Sink: KSTREAM-SINK-0000000003 (topic: output)
    	  <-- KSTREAM-MAPVALUES-0000000002

From this report, you can see what the different operators are, but what is the broader context here? For example, consider `KSTREAM-FILTER-0000000001`, we can see that it's a filter operation, which means that records are dropped that don't match the given predicate. But what is the meaning of the predicate? Additionally, you can see the topic names of the source and sink nodes, but what if the topics aren't named in a meaningful way? Then you're left to guess the business purpose behind these topics. 

Also notice the numbering here: the source node is suffixed with `0000000000` indicating it's the first processor in the topology. The filter is suffixed with `0000000001`, indicating it's the second processor in the topology. In Kafka Streams, there are now overloaded methods for both `KStream` and `KTable` that accept a new parameter `Named`. By using the `Named` class DSL users can provide meaningful names to the processors in their topology. 

Now let's take a look at your topology with all the processors named: 
    
    
    KStream<String,String> stream =
    builder.stream("input", Consumed.as("Customer_transactions_input_topic"));
    stream.filter((k,v) -> !v.equals("invalid_txn"), Named.as("filter_out_invalid_txns"))
    	  .mapValues((v) -> v.substring(0,5), Named.as("Map_values_to_first_6_characters"))
    	  .to("output", Produced.as("Mapped_transactions_output_topic"));
    
    
    Topologies:
       Sub-topology: 0
    	Source: Customer_transactions_input_topic (topics: [input])
    	  --> filter_out_invalid_txns
    	Processor: filter_out_invalid_txns (stores: [])
    	  --> Map_values_to_first_6_characters
    	  <-- Customer_transactions_input_topic
    	Processor: Map_values_to_first_6_characters (stores: [])
    	  --> Mapped_transactions_output_topic
    	  <-- filter_out_invalid_txns
    	Sink: Mapped_transactions_output_topic (topic: output)
    	  <-- Map_values_to_first_6_characters

Now you can look at the topology description and easily understand what role each processor plays in the topology. But there's another reason for naming your processor nodes when you have stateful operators that remain between restarts of your Kafka Streams applications, state stores, changelog topics, and repartition topics. 

# Changing Names

Generated names are numbered where they are built in the topology. The name generation strategy is `KSTREAM|KTABLE->operator name<->number suffix<`. The number is a globally incrementing number that represents the operator's order in the topology. The generated number is prefixed with a varying number of "0"s to create a string that is consistently 10 characters long. This means that if you add/remove or shift the order of operations, the position of the processor shifts, which shifts the name of the processor. Since **most** processors exist in memory only, this name shifting presents no issue for many topologies. But the name shifting does have implications for topologies with stateful operators or repartition topics. Here's a different topology with some state: 
    
    
    KStream<String,String> stream = builder.stream("input");
     stream.groupByKey()
    	   .count()
    	   .toStream()
    	   .to("output");

This topology description yields the following: 
    
    
    Topologies:
       Sub-topology: 0
    	Source: KSTREAM-SOURCE-0000000000 (topics: [input])
    	 --> KSTREAM-AGGREGATE-0000000002
    	Processor: KSTREAM-AGGREGATE-0000000002 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000001])
    	 --> KTABLE-TOSTREAM-0000000003
    	 <-- KSTREAM-SOURCE-0000000000
    	Processor: KTABLE-TOSTREAM-0000000003 (stores: [])
    	 --> KSTREAM-SINK-0000000004
    	 <-- KSTREAM-AGGREGATE-0000000002
    	Sink: KSTREAM-SINK-0000000004 (topic: output)
    	 <-- KTABLE-TOSTREAM-0000000003

You can see from the topology description above that the state store is named `KSTREAM-AGGREGATE-STATE-STORE-0000000002`. Here's what happens when you add a filter to keep some of the records out of the aggregation: 
    
    
    KStream<String,String> stream = builder.stream("input");
    stream.filter((k,v)-> v !=null && v.length() >= 6 )
          .groupByKey()
          .count()
          .toStream()
          .to("output");

And the corresponding topology: 
    
    
    Topologies:
    	Sub-topology: 0
    	 Source: KSTREAM-SOURCE-0000000000 (topics: [input])
    	  --> KSTREAM-FILTER-0000000001
    	 Processor: KSTREAM-FILTER-0000000001 (stores: [])
    	   --> KSTREAM-AGGREGATE-0000000003
    	   <-- KSTREAM-SOURCE-0000000000
    	 Processor: KSTREAM-AGGREGATE-0000000003 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000002])
    	   --> KTABLE-TOSTREAM-0000000004
    	   <-- KSTREAM-FILTER-0000000001
    	 Processor: KTABLE-TOSTREAM-0000000004 (stores: [])
    	   --> KSTREAM-SINK-0000000005
    	   <-- KSTREAM-AGGREGATE-0000000003
    	  Sink: KSTREAM-SINK-0000000005 (topic: output)
    	   <-- KTABLE-TOSTREAM-0000000004

Notice that since you've added an operation _before_ the `count` operation, the state store (and the changelog topic) names have changed. This name change means you can't do a rolling re-deployment of your updated topology. Also, you must use the [Streams Reset Tool](/39/documentation/streams/developer-guide/app-reset-tool) to re-calculate the aggregations, because the changelog topic has changed on start-up and the new changelog topic contains no data. Fortunately, there's an easy solution to remedy this situation. Give the state store a user-defined name instead of relying on the generated one, so you don't have to worry about topology changes shifting the name of the state store. You've had the ability to name repartition topics with the `Joined`, `StreamJoined`, and`Grouped` classes, and name state store and changelog topics with `Materialized`. But it's worth reiterating the importance of naming these DSL topology operations again. Here's how your DSL code looks now giving a specific name to your state store: 
    
    
    KStream<String,String> stream = builder.stream("input");
    stream.filter((k, v) -> v != null && v.length() >= 6)
    	  .groupByKey()
    	  .count(Materialized.as("Purchase_count_store"))
    	  .toStream()
    	  .to("output");

And here's the topology 
    
    
    Topologies:
       Sub-topology: 0
    	Source: KSTREAM-SOURCE-0000000000 (topics: [input])
    	  --> KSTREAM-FILTER-0000000001
    	Processor: KSTREAM-FILTER-0000000001 (stores: [])
    	  --> KSTREAM-AGGREGATE-0000000002
    	  <-- KSTREAM-SOURCE-0000000000
    	Processor: KSTREAM-AGGREGATE-0000000002 (stores: [Purchase_count_store])
    	  --> KTABLE-TOSTREAM-0000000003
    	  <-- KSTREAM-FILTER-0000000001
    	Processor: KTABLE-TOSTREAM-0000000003 (stores: [])
    	  --> KSTREAM-SINK-0000000004
    	  <-- KSTREAM-AGGREGATE-0000000002
    	Sink: KSTREAM-SINK-0000000004 (topic: output)
    	  <-- KTABLE-TOSTREAM-0000000003

Now, even though you've added processors before your state store, the store name and its changelog topic names don't change. This makes your topology more robust and resilient to changes made by adding or removing processors. 

# Conclusion

It's a good practice to name your processing nodes when using the DSL, and it's even more important to do this when you have "stateful" processors your application such as repartition topics and state stores (and the accompanying changelog topics). 

Here are a couple of points to remember when naming your DSL topology: 

  1. If you have an _existing topology_ and you _haven't_ named your state stores (and changelog topics) and repartition topics, we recommended that you do so. But this will be a topology breaking change, so you'll need to shut down all application instances, make the changes, and run the [Streams Reset Tool](/39/documentation/streams/developer-guide/app-reset-tool). Although this may be inconvenient at first, it's worth the effort to protect your application from unexpected errors due to topology changes. 
  2. If you have a _new topology_ , make sure you name the persistent parts of your topology: state stores (changelog topics) and repartition topics. This way, when you deploy your application, you're protected from topology changes that otherwise would break your Kafka Streams application. If you don't want to add names to stateless processors at first, that's fine as you can always go back and add the names later. 

Here's a quick reference on naming the critical parts of your Kafka Streams application to prevent topology name changes from breaking your application:   
<table>  
<tr>  
<th>

Operation
</th>  
<th>

Naming Class
</th> </tr>  
<tr>  
<td>

Aggregation repartition topics
</td>  
<td>

Grouped
</td> </tr>  
<tr>  
<td>

KStream-KStream Join repartition topics
</td>  
<td>

StreamJoined
</td> </tr>  
<tr>  
<td>

KStream-KTable Join repartition topic
</td>  
<td>

Joined
</td> </tr>  
<tr>  
<td>

KStream-KStream Join state stores
</td>  
<td>

StreamJoined
</td> </tr>  
<tr>  
<td>

State Stores (for aggregations and KTable-KTable joins)
</td>  
<td>

Materialized
</td> </tr>  
<tr>  
<td>

Stream/Table non-stateful operations
</td>  
<td>

Named
</td> </tr> </table>

  * [Documentation](/documentation)
  * [Kafka Streams](/documentation/streams)
  * [Developer Guide](/documentation/streams/developer-guide/)


