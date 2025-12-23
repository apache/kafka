---
title: Multi-Tenancy
description: Multi-Tenancy
weight: 4
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


## Multi-Tenancy Overview

As a highly scalable event streaming platform, Kafka is used by many users as their central nervous system, connecting in real-time a wide range of different systems and applications from various teams and lines of businesses. Such multi-tenant cluster environments command proper control and management to ensure the peaceful coexistence of these different needs. This section highlights features and best practices to set up such shared environments, which should help you operate clusters that meet SLAs/OLAs and that minimize potential collateral damage caused by "noisy neighbors". 

Multi-tenancy is a many-sided subject, including but not limited to: 

  * Creating user spaces for tenants (sometimes called namespaces)
  * Configuring topics with data retention policies and more
  * Securing topics and clusters with encryption, authentication, and authorization
  * Isolating tenants with quotas and rate limits
  * Monitoring and metering
  * Inter-cluster data sharing (cf. geo-replication)



## Creating User Spaces (Namespaces) For Tenants With Topic Naming

Kafka administrators operating a multi-tenant cluster typically need to define user spaces for each tenant. For the purpose of this section, "user spaces" are a collection of topics, which are grouped together under the management of a single entity or user. 

In Kafka, the main unit of data is the topic. Users can create and name each topic. They can also delete them, but it is not possible to rename a topic directly. Instead, to rename a topic, the user must create a new topic, move the messages from the original topic to the new, and then delete the original. With this in mind, it is recommended to define logical spaces, based on an hierarchical topic naming structure. This setup can then be combined with security features, such as prefixed ACLs, to isolate different spaces and tenants, while also minimizing the administrative overhead for securing the data in the cluster. 

These logical user spaces can be grouped in different ways, and the concrete choice depends on how your organization prefers to use your Kafka clusters. The most common groupings are as follows. 

_By team or organizational unit:_ Here, the team is the main aggregator. In an organization where teams are the main user of the Kafka infrastructure, this might be the best grouping. 

Example topic naming structure: 

  * `<organization>.<team>.<dataset>.<event-name>`  
(e.g., "acme.infosec.telemetry.logins")



_By project or product:_ Here, a team manages more than one project. Their credentials will be different for each project, so all the controls and settings will always be project related. 

Example topic naming structure: 

  * `<project>.<product>.<event-name>`  
(e.g., "mobility.payments.suspicious")



Certain information should normally not be put in a topic name, such as information that is likely to change over time (e.g., the name of the intended consumer) or that is a technical detail or metadata that is available elsewhere (e.g., the topic's partition count and other configuration settings). 

To enforce a topic naming structure, several options are available: 

  * Use prefix ACLs (cf. [KIP-290](https://cwiki.apache.org/confluence/x/QpvLB)) to enforce a common prefix for topic names. For example, team A may only be permitted to create topics whose names start with `payments.teamA.`.
  * Define a custom `CreateTopicPolicy` (cf. [KIP-108](https://cwiki.apache.org/confluence/x/Iw8IB) and the setting create.topic.policy.class.name) to enforce strict naming patterns. These policies provide the most flexibility and can cover complex patterns and rules to match an organization's needs.
  * Disable topic creation for normal users by denying it with an ACL, and then rely on an external process to create topics on behalf of users (e.g., scripting or your favorite automation toolkit).
  * It may also be useful to disable the Kafka feature to auto-create topics on demand by setting `auto.create.topics.enable=false` in the broker configuration. Note that you should not rely solely on this option.



## Configuring Topics: Data Retention And More

Kafka's configuration is very flexible due to its fine granularity, and it supports a plethora of per-topic configuration settings to help administrators set up multi-tenant clusters. For example, administrators often need to define data retention policies to control how much and/or for how long data will be stored in a topic, with settings such as retention.bytes (size) and retention.ms (time). This limits storage consumption within the cluster, and helps complying with legal requirements such as GDPR. 

## Securing Clusters and Topics: Authentication, Authorization, Encryption

Because the documentation has a dedicated chapter on security that applies to any Kafka deployment, this section focuses on additional considerations for multi-tenant environments. 

Security settings for Kafka fall into three main categories, which are similar to how administrators would secure other client-server data systems, like relational databases and traditional messaging systems. 

  1. **Encryption** of data transferred between Kafka brokers and Kafka clients, between brokers, and between brokers and other optional tools.
  2. **Authentication** of connections from Kafka clients and applications to Kafka brokers, as well as connections between Kafka brokers.
  3. **Authorization** of client operations such as creating, deleting, and altering the configuration of topics; writing events to or reading events from a topic; creating and deleting ACLs. Administrators can also define custom policies to put in place additional restrictions, such as a `CreateTopicPolicy` and `AlterConfigPolicy` (see [KIP-108](https://cwiki.apache.org/confluence/x/Iw8IB) and the settings create.topic.policy.class.name, alter.config.policy.class.name).



When securing a multi-tenant Kafka environment, the most common administrative task is the third category (authorization), i.e., managing the user/client permissions that grant or deny access to certain topics and thus to the data stored by users within a cluster. This task is performed predominantly through the setting of access control lists (ACLs). Here, administrators of multi-tenant environments in particular benefit from putting a hierarchical topic naming structure in place as described in a previous section, because they can conveniently control access to topics through prefixed ACLs (`--resource-pattern-type Prefixed`). This significantly minimizes the administrative overhead of securing topics in multi-tenant environments: administrators can make their own trade-offs between higher developer convenience (more lenient permissions, using fewer and broader ACLs) vs. tighter security (more stringent permissions, using more and narrower ACLs). 

In the following example, user Alice-a new member of ACME corporation's InfoSec team-is granted write permissions to all topics whose names start with "acme.infosec.", such as "acme.infosec.telemetry.logins" and "acme.infosec.syslogs.events". 
    
    
    # Grant permissions to user Alice
    $ bin/kafka-acls.sh \
        --bootstrap-server localhost:9092 \
        --add --allow-principal User:Alice \
        --producer \
        --resource-pattern-type prefixed --topic acme.infosec.

You can similarly use this approach to isolate different customers on the same shared cluster. 

## Isolating Tenants: Quotas, Rate Limiting, Throttling

Multi-tenant clusters should generally be configured with quotas, which protect against users (tenants) eating up too many cluster resources, such as when they attempt to write or read very high volumes of data, or create requests to brokers at an excessively high rate. This may cause network saturation, monopolize broker resources, and impact other clients-all of which you want to avoid in a shared environment. 

**Client quotas:** Kafka supports different types of (per-user principal) client quotas. Because a client's quotas apply irrespective of which topics the client is writing to or reading from, they are a convenient and effective tool to allocate resources in a multi-tenant cluster. Request rate quotas, for example, help to limit a user's impact on broker CPU usage by limiting the time a broker spends on the [request handling path](/protocol.html) for that user, after which throttling kicks in. In many situations, isolating users with request rate quotas has a bigger impact in multi-tenant clusters than setting incoming/outgoing network bandwidth quotas, because excessive broker CPU usage for processing requests reduces the effective bandwidth the broker can serve. Furthermore, administrators can also define quotas on topic operations-such as create, delete, and alter-to prevent Kafka clusters from being overwhelmed by highly concurrent topic operations (see [KIP-599](https://cwiki.apache.org/confluence/x/6DLcC) and the quota type `controller_mutation_rate`). 

**Server quotas:** Kafka also supports different types of broker-side quotas. For example, administrators can set a limit on the rate with which the broker accepts new connections, set the maximum number of connections per broker, or set the maximum number of connections allowed from a specific IP address. 

For more information, please refer to the quota overview and how to set quotas. 

## Monitoring and Metering

Monitoring is a broader subject that is covered elsewhere in the documentation. Administrators of any Kafka environment, but especially multi-tenant ones, should set up monitoring according to these instructions. Kafka supports a wide range of metrics, such as the rate of failed authentication attempts, request latency, consumer lag, total number of consumer groups, metrics on the quotas described in the previous section, and many more. 

For example, monitoring can be configured to track the size of topic-partitions (with the JMX metric `kafka.log.Log.Size.<TOPIC-NAME>`), and thus the total size of data stored in a topic. You can then define alerts when tenants on shared clusters are getting close to using too much storage space. 

## Multi-Tenancy and Geo-Replication

Kafka lets you share data across different clusters, which may be located in different geographical regions, data centers, and so on. Apart from use cases such as disaster recovery, this functionality is useful when a multi-tenant setup requires inter-cluster data sharing. See the section Geo-Replication (Cross-Cluster Data Mirroring) for more information. 

## Further considerations

**Data contracts:** You may need to define data contracts between the producers and the consumers of data in a cluster, using event schemas. This ensures that events written to Kafka can always be read properly again, and prevents malformed or corrupt events being written. The best way to achieve this is to deploy a so-called schema registry alongside the cluster. (Kafka does not include a schema registry, but there are third-party implementations available.) A schema registry manages the event schemas and maps the schemas to topics, so that producers know which topics are accepting which types (schemas) of events, and consumers know how to read and parse events in a topic. Some registry implementations provide further functionality, such as schema evolution, storing a history of all schemas, and schema compatibility settings. 
