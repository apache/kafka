---
title: Authorization and ACLs
description: Authorization and ACLs
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


Kafka ships with a pluggable authorization framework, which is configured with the `authorizer.class.name` property in the server configuration. Configured implementations must extend `org.apache.kafka.server.authorizer.Authorizer`. Kafka provides default implementations which store ACLs in the cluster metadata (either Zookeeper or the KRaft metadata log). For Zookeeper-based clusters, the provided implementation is configured as follows: 
    
    
    authorizer.class.name=kafka.security.authorizer.AclAuthorizer

For KRaft clusters, use the following configuration on all nodes (brokers, controllers, or combined broker/controller nodes): 
    
    
    authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer

Kafka ACLs are defined in the general format of "Principal {P} is [Allowed|Denied] Operation {O} From Host {H} on any Resource {R} matching ResourcePattern {RP}". You can read more about the ACL structure in [KIP-11](https://cwiki.apache.org/confluence/display/KAFKA/KIP-11+-+Authorization+Interface) and resource patterns in [KIP-290](https://cwiki.apache.org/confluence/display/KAFKA/KIP-290%3A+Support+for+Prefixed+ACLs). In order to add, remove, or list ACLs, you can use the Kafka ACL CLI `kafka-acls.sh`. By default, if no ResourcePatterns match a specific Resource R, then R has no associated ACLs, and therefore no one other than super users is allowed to access R. If you want to change that behavior, you can include the following in server.properties. 
    
    
    allow.everyone.if.no.acl.found=true

One can also add super users in server.properties like the following (note that the delimiter is semicolon since SSL user names may contain comma). Default PrincipalType string "User" is case sensitive. 
    
    
    super.users=User:Bob;User:Alice

### KRaft Principal Forwarding

In KRaft clusters, admin requests such as `CreateTopics` and `DeleteTopics` are sent to the broker listeners by the client. The broker then forwards the request to the active controller through the first listener configured in `controller.listener.names`. Authorization of these requests is done on the controller node. This is achieved by way of an `Envelope` request which packages both the underlying request from the client as well as the client principal. When the controller receives the forwarded `Envelope` request from the broker, it first authorizes the `Envelope` request using the authenticated broker principal. Then it authorizes the underlying request using the forwarded principal.   
All of this implies that Kafka must understand how to serialize and deserialize the client principal. The authentication framework allows for customized principals by overriding the `principal.builder.class` configuration. In order for customized principals to work with KRaft, the configured class must implement `org.apache.kafka.common.security.auth.KafkaPrincipalSerde` so that Kafka knows how to serialize and deserialize the principals. The default implementation `org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder` uses the Kafka RPC format defined in the source code: `clients/src/main/resources/common/message/DefaultPrincipalData.json`. For more detail about request forwarding in KRaft, see [KIP-590](https://cwiki.apache.org/confluence/display/KAFKA/KIP-590%3A+Redirect+Zookeeper+Mutation+Protocols+to+The+Controller)

### Customizing SSL User Name

By default, the SSL user name will be of the form "CN=writeuser,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown". One can change that by setting `ssl.principal.mapping.rules` to a customized rule in server.properties. This config allows a list of rules for mapping X.500 distinguished name to short name. The rules are evaluated in order and the first rule that matches a distinguished name is used to map it to a short name. Any later rules in the list are ignored.   
The format of `ssl.principal.mapping.rules` is a list where each rule starts with "RULE:" and contains an expression as the following formats. Default rule will return string representation of the X.500 certificate distinguished name. If the distinguished name matches the pattern, then the replacement command will be run over the name. This also supports lowercase/uppercase options, to force the translated result to be all lower/uppercase case. This is done by adding a "/L" or "/U' to the end of the rule. 
    
    
    RULE:pattern/replacement/
    RULE:pattern/replacement/[LU]

Example `ssl.principal.mapping.rules` values are: 
    
    
    RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/,
    RULE:^CN=(.*?),OU=(.*?),O=(.*?),L=(.*?),ST=(.*?),C=(.*?)$/$1@$2/L,
    RULE:^.*[Cc][Nn]=([a-zA-Z0-9.]*).*$/$1/L,
    DEFAULT

Above rules translate distinguished name "CN=serviceuser,OU=ServiceUsers,O=Unknown,L=Unknown,ST=Unknown,C=Unknown" to "serviceuser" and "CN=adminUser,OU=Admin,O=Unknown,L=Unknown,ST=Unknown,C=Unknown" to "adminuser@admin".   
For advanced use cases, one can customize the name by setting a customized PrincipalBuilder in server.properties like the following. 
    
    
    principal.builder.class=CustomizedPrincipalBuilderClass

### Customizing SASL User Name

By default, the SASL user name will be the primary part of the Kerberos principal. One can change that by setting `sasl.kerberos.principal.to.local.rules` to a customized rule in server.properties. The format of `sasl.kerberos.principal.to.local.rules` is a list where each rule works in the same way as the auth_to_local in [Kerberos configuration file (krb5.conf)](https://web.mit.edu/Kerberos/krb5-latest/doc/admin/conf_files/krb5_conf.html). This also support additional lowercase/uppercase rule, to force the translated result to be all lowercase/uppercase. This is done by adding a "/L" or "/U" to the end of the rule. check below formats for syntax. Each rules starts with RULE: and contains an expression as the following formats. See the kerberos documentation for more details. 
    
    
    RULE:[n:string](regexp)s/pattern/replacement/
    RULE:[n:string](regexp)s/pattern/replacement/g
    RULE:[n:string](regexp)s/pattern/replacement//L
    RULE:[n:string](regexp)s/pattern/replacement/g/L
    RULE:[n:string](regexp)s/pattern/replacement//U
    RULE:[n:string](regexp)s/pattern/replacement/g/U

An example of adding a rule to properly translate user@MYDOMAIN.COM to user while also keeping the default rule in place is: 
    
    
    sasl.kerberos.principal.to.local.rules=RULE:[1:$1@$0](.*@MYDOMAIN.COM)s/@.*//,DEFAULT

## Command Line Interface

Kafka Authorization management CLI can be found under bin directory with all the other CLIs. The CLI script is called **kafka-acls.sh**. Following lists all the options that the script supports:   
  
<table>  
<tr>  
<th>

Option
</th>  
<th>

Description
</th>  
<th>

Default
</th>  
<th>

Option type
</th> </tr>  
<tr>  
<td>

\--add
</td>  
<td>

Indicates to the script that user is trying to add an acl.
</td>  
<td>


</td>  
<td>

Action
</td> </tr>  
<tr>  
<td>

\--remove
</td>  
<td>

Indicates to the script that user is trying to remove an acl.
</td>  
<td>


</td>  
<td>

Action
</td> </tr>  
<tr>  
<td>

\--list
</td>  
<td>

Indicates to the script that user is trying to list acls.
</td>  
<td>


</td>  
<td>

Action
</td> </tr>  
<tr>  
<td>

\--bootstrap-server
</td>  
<td>

A list of host/port pairs to use for establishing the connection to the Kafka cluster. Only one of --bootstrap-server or --authorizer option must be specified.
</td>  
<td>


</td>  
<td>

Configuration
</td> </tr>  
<tr>  
<td>

\--command-config
</td>  
<td>

A property file containing configs to be passed to Admin Client. This option can only be used with --bootstrap-server option.
</td>  
<td>


</td>  
<td>

Configuration
</td> </tr>  
<tr>  
<td>

\--cluster
</td>  
<td>

Indicates to the script that the user is trying to interact with acls on the singular cluster resource.
</td>  
<td>


</td>  
<td>

ResourcePattern
</td> </tr>  
<tr>  
<td>

\--topic [topic-name]
</td>  
<td>

Indicates to the script that the user is trying to interact with acls on topic resource pattern(s).
</td>  
<td>


</td>  
<td>

ResourcePattern
</td> </tr>  
<tr>  
<td>

\--group [group-name]
</td>  
<td>

Indicates to the script that the user is trying to interact with acls on consumer-group resource pattern(s)
</td>  
<td>


</td>  
<td>

ResourcePattern
</td> </tr>  
<tr>  
<td>

\--transactional-id [transactional-id]
</td>  
<td>

The transactionalId to which ACLs should be added or removed. A value of * indicates the ACLs should apply to all transactionalIds.
</td>  
<td>


</td>  
<td>

ResourcePattern
</td> </tr>  
<tr>  
<td>

\--delegation-token [delegation-token]
</td>  
<td>

Delegation token to which ACLs should be added or removed. A value of * indicates ACL should apply to all tokens.
</td>  
<td>


</td>  
<td>

ResourcePattern
</td> </tr>  
<tr>  
<td>

\--user-principal [user-principal]
</td>  
<td>

A user resource to which ACLs should be added or removed. This is currently supported in relation with delegation tokens. A value of * indicates ACL should apply to all users.
</td>  
<td>


</td>  
<td>

ResourcePattern
</td> </tr>  
<tr>  
<td>

\--resource-pattern-type [pattern-type]
</td>  
<td>

Indicates to the script the type of resource pattern, (for --add), or resource pattern filter, (for --list and --remove), the user wishes to use.  
When adding acls, this should be a specific pattern type, e.g. 'literal' or 'prefixed'.  
When listing or removing acls, a specific pattern type filter can be used to list or remove acls from a specific type of resource pattern, or the filter values of 'any' or 'match' can be used, where 'any' will match any pattern type, but will match the resource name exactly, and 'match' will perform pattern matching to list or remove all acls that affect the supplied resource(s).  
WARNING: 'match', when used in combination with the '--remove' switch, should be used with care. 
</td>  
<td>

literal
</td>  
<td>

Configuration
</td> </tr>  
<tr>  
<td>

\--allow-principal
</td>  
<td>

Principal is in PrincipalType:name format that will be added to ACL with Allow permission. Default PrincipalType string "User" is case sensitive.   
You can specify multiple --allow-principal in a single command.
</td>  
<td>


</td>  
<td>

Principal
</td> </tr>  
<tr>  
<td>

\--deny-principal
</td>  
<td>

Principal is in PrincipalType:name format that will be added to ACL with Deny permission. Default PrincipalType string "User" is case sensitive.   
You can specify multiple --deny-principal in a single command.
</td>  
<td>


</td>  
<td>

Principal
</td> </tr>  
<tr>  
<td>

\--principal
</td>  
<td>

Principal is in PrincipalType:name format that will be used along with --list option. Default PrincipalType string "User" is case sensitive. This will list the ACLs for the specified principal.   
You can specify multiple --principal in a single command.
</td>  
<td>


</td>  
<td>

Principal
</td> </tr>  
<tr>  
<td>

\--allow-host
</td>  
<td>

IP address from which principals listed in --allow-principal will have access.
</td>  
<td>

if --allow-principal is specified defaults to * which translates to "all hosts"
</td>  
<td>

Host
</td> </tr>  
<tr>  
<td>

\--deny-host
</td>  
<td>

IP address from which principals listed in --deny-principal will be denied access.
</td>  
<td>

if --deny-principal is specified defaults to * which translates to "all hosts"
</td>  
<td>

Host
</td> </tr>  
<tr>  
<td>

\--operation
</td>  
<td>

Operation that will be allowed or denied.  
Valid values are: 

  * Read
  * Write
  * Create
  * Delete
  * Alter
  * Describe
  * ClusterAction
  * DescribeConfigs
  * AlterConfigs
  * IdempotentWrite
  * CreateTokens
  * DescribeTokens
  * All


</td>  
<td>

All
</td>  
<td>

Operation
</td> </tr>  
<tr>  
<td>

\--producer
</td>  
<td>

Convenience option to add/remove acls for producer role. This will generate acls that allows WRITE, DESCRIBE and CREATE on topic.
</td>  
<td>


</td>  
<td>

Convenience
</td> </tr>  
<tr>  
<td>

\--consumer
</td>  
<td>

Convenience option to add/remove acls for consumer role. This will generate acls that allows READ, DESCRIBE on topic and READ on consumer-group.
</td>  
<td>


</td>  
<td>

Convenience
</td> </tr>  
<tr>  
<td>

\--idempotent
</td>  
<td>

Enable idempotence for the producer. This should be used in combination with the --producer option.  
Note that idempotence is enabled automatically if the producer is authorized to a particular transactional-id. 
</td>  
<td>


</td>  
<td>

Convenience
</td> </tr>  
<tr>  
<td>

\--force
</td>  
<td>

Convenience option to assume yes to all queries and do not prompt.
</td>  
<td>


</td>  
<td>

Convenience
</td> </tr>  
<tr>  
<td>

\--authorizer
</td>  
<td>

(DEPRECATED: not supported in KRaft) Fully qualified class name of the authorizer.
</td>  
<td>

kafka.security.authorizer.AclAuthorizer
</td>  
<td>

Configuration
</td> </tr>  
<tr>  
<td>

\--authorizer-properties
</td>  
<td>

(DEPRECATED: not supported in KRaft) key=val pairs that will be passed to authorizer for initialization. For the default authorizer in ZK clsuters, the example values are: zookeeper.connect=localhost:2181
</td>  
<td>


</td>  
<td>

Configuration
</td> </tr>  
<tr>  
<td>

\--zk-tls-config-file
</td>  
<td>

(DEPRECATED: not supported in KRaft) Identifies the file where ZooKeeper client TLS connectivity properties for the authorizer are defined. Any properties other than the following (with or without an "authorizer." prefix) are ignored: zookeeper.clientCnxnSocket, zookeeper.ssl.cipher.suites, zookeeper.ssl.client.enable, zookeeper.ssl.crl.enable, zookeeper.ssl.enabled.protocols, zookeeper.ssl.endpoint.identification.algorithm, zookeeper.ssl.keystore.location, zookeeper.ssl.keystore.password, zookeeper.ssl.keystore.type, zookeeper.ssl.ocsp.enable, zookeeper.ssl.protocol, zookeeper.ssl.truststore.location, zookeeper.ssl.truststore.password, zookeeper.ssl.truststore.type 
</td>  
<td>


</td>  
<td>

Configuration
</td> </tr> </table>

## Examples

  * **Adding Acls**  
Suppose you want to add an acl "Principals User:Bob and User:Alice are allowed to perform Operation Read and Write on Topic Test-Topic from IP 198.51.100.0 and IP 198.51.100.1". You can do that by executing the CLI with following options: 
        
        $ bin/kafka-acls.sh --bootstrap-server localhost:9092 --add --allow-principal User:Bob --allow-principal User:Alice --allow-host 198.51.100.0 --allow-host 198.51.100.1 --operation Read --operation Write --topic Test-topic

By default, all principals that don't have an explicit acl that allows access for an operation to a resource are denied. In rare cases where an allow acl is defined that allows access to all but some principal we will have to use the --deny-principal and --deny-host option. For example, if we want to allow all users to Read from Test-topic but only deny User:BadBob from IP 198.51.100.3 we can do so using following commands: 
        
        $ bin/kafka-acls.sh --bootstrap-server localhost:9092 --add --allow-principal User:'*' --allow-host '*' --deny-principal User:BadBob --deny-host 198.51.100.3 --operation Read --topic Test-topic

Note that `--allow-host` and `--deny-host` only support IP addresses (hostnames are not supported). Above examples add acls to a topic by specifying --topic [topic-name] as the resource pattern option. Similarly user can add acls to cluster by specifying --cluster and to a consumer group by specifying --group [group-name]. You can add acls on any resource of a certain type, e.g. suppose you wanted to add an acl "Principal User:Peter is allowed to produce to any Topic from IP 198.51.200.0" You can do that by using the wildcard resource '*', e.g. by executing the CLI with following options: 
        
        $ bin/kafka-acls.sh --bootstrap-server localhost:9092 --add --allow-principal User:Peter --allow-host 198.51.200.1 --producer --topic '*'

You can add acls on prefixed resource patterns, e.g. suppose you want to add an acl "Principal User:Jane is allowed to produce to any Topic whose name starts with 'Test-' from any host". You can do that by executing the CLI with following options: 
        
        $ bin/kafka-acls.sh --bootstrap-server localhost:9092 --add --allow-principal User:Jane --producer --topic Test- --resource-pattern-type prefixed

Note, --resource-pattern-type defaults to 'literal', which only affects resources with the exact same name or, in the case of the wildcard resource name '*', a resource with any name.
  * **Removing Acls**  
Removing acls is pretty much the same. The only difference is instead of --add option users will have to specify --remove option. To remove the acls added by the first example above we can execute the CLI with following options: 
        
        $ bin/kafka-acls.sh --bootstrap-server localhost:9092 --remove --allow-principal User:Bob --allow-principal User:Alice --allow-host 198.51.100.0 --allow-host 198.51.100.1 --operation Read --operation Write --topic Test-topic 

If you want to remove the acl added to the prefixed resource pattern above we can execute the CLI with following options: 
        
        $ bin/kafka-acls.sh --bootstrap-server localhost:9092 --remove --allow-principal User:Jane --producer --topic Test- --resource-pattern-type Prefixed

  * **List Acls**  
We can list acls for any resource by specifying the --list option with the resource. To list all acls on the literal resource pattern Test-topic, we can execute the CLI with following options: 
        
        $ bin/kafka-acls.sh --bootstrap-server localhost:9092 --list --topic Test-topic

However, this will only return the acls that have been added to this exact resource pattern. Other acls can exist that affect access to the topic, e.g. any acls on the topic wildcard '*', or any acls on prefixed resource patterns. Acls on the wildcard resource pattern can be queried explicitly: 
        
        $ bin/kafka-acls.sh --bootstrap-server localhost:9092 --list --topic '*'

However, it is not necessarily possible to explicitly query for acls on prefixed resource patterns that match Test-topic as the name of such patterns may not be known. We can list _all_ acls affecting Test-topic by using '--resource-pattern-type match', e.g. 
        
        > bin/kafka-acls.sh --bootstrap-server localhost:9092 --list --topic Test-topic --resource-pattern-type match

This will list acls on all matching literal, wildcard and prefixed resource patterns.
  * **Adding or removing a principal as producer or consumer**  
The most common use case for acl management are adding/removing a principal as producer or consumer so we added convenience options to handle these cases. In order to add User:Bob as a producer of Test-topic we can execute the following command: 
        
        $ bin/kafka-acls.sh --bootstrap-server localhost:9092 --add --allow-principal User:Bob --producer --topic Test-topic

Similarly to add Alice as a consumer of Test-topic with consumer group Group-1 we just have to pass --consumer option: 
        
        $ bin/kafka-acls.sh --bootstrap-server localhost:9092 --add --allow-principal User:Bob --consumer --topic Test-topic --group Group-1 

Note that for consumer option we must also specify the consumer group. In order to remove a principal from producer or consumer role we just need to pass --remove option. 
  * **Admin API based acl management**  
Users having Alter permission on ClusterResource can use Admin API for ACL management. kafka-acls.sh script supports AdminClient API to manage ACLs without interacting with zookeeper/authorizer directly. All the above examples can be executed by using **\--bootstrap-server** option. For example: 
        
        $ bin/kafka-acls.sh --bootstrap-server localhost:9092 --command-config /tmp/adminclient-configs.conf --add --allow-principal User:Bob --producer --topic Test-topic
        $ bin/kafka-acls.sh --bootstrap-server localhost:9092 --command-config /tmp/adminclient-configs.conf --add --allow-principal User:Bob --consumer --topic Test-topic --group Group-1
        $ bin/kafka-acls.sh --bootstrap-server localhost:9092 --command-config /tmp/adminclient-configs.conf --list --topic Test-topic
        $ bin/kafka-acls.sh --bootstrap-server localhost:9092 --command-config /tmp/adminclient-configs.conf --add --allow-principal User:tokenRequester --operation CreateTokens --user-principal "owner1"




## Authorization Primitives

Protocol calls are usually performing some operations on certain resources in Kafka. It is required to know the operations and resources to set up effective protection. In this section we'll list these operations and resources, then list the combination of these with the protocols to see the valid scenarios.

### Operations in Kafka

There are a few operation primitives that can be used to build up privileges. These can be matched up with certain resources to allow specific protocol calls for a given user. These are:

  * Read
  * Write
  * Create
  * Delete
  * Alter
  * Describe
  * ClusterAction
  * DescribeConfigs
  * AlterConfigs
  * IdempotentWrite
  * CreateTokens
  * DescribeTokens
  * All



### Resources in Kafka

The operations above can be applied on certain resources which are described below.

  * **Topic:** this simply represents a Topic. All protocol calls that are acting on topics (such as reading, writing them) require the corresponding privilege to be added. If there is an authorization error with a topic resource, then a TOPIC_AUTHORIZATION_FAILED (error code: 29) will be returned.
  * **Group:** this represents the consumer groups in the brokers. All protocol calls that are working with consumer groups, like joining a group must have privileges with the group in subject. If the privilege is not given then a GROUP_AUTHORIZATION_FAILED (error code: 30) will be returned in the protocol response.
  * **Cluster:** this resource represents the cluster. Operations that are affecting the whole cluster, like controlled shutdown are protected by privileges on the Cluster resource. If there is an authorization problem on a cluster resource, then a CLUSTER_AUTHORIZATION_FAILED (error code: 31) will be returned.
  * **TransactionalId:** this resource represents actions related to transactions, such as committing. If any error occurs, then a TRANSACTIONAL_ID_AUTHORIZATION_FAILED (error code: 53) will be returned by brokers.
  * **DelegationToken:** this represents the delegation tokens in the cluster. Actions, such as describing delegation tokens could be protected by a privilege on the DelegationToken resource. Since these objects have a little special behavior in Kafka it is recommended to read [KIP-48](https://cwiki.apache.org/confluence/display/KAFKA/KIP-48+Delegation+token+support+for+Kafka#KIP-48DelegationtokensupportforKafka-DescribeDelegationTokenRequest) and the related upstream documentation at Authentication using Delegation Tokens.
  * **User:** CreateToken and DescribeToken operations can be granted to User resources to allow creating and describing tokens for other users. More info can be found in [KIP-373](https://cwiki.apache.org/confluence/display/KAFKA/KIP-373%3A+Allow+users+to+create+delegation+tokens+for+other+users).



### Operations and Resources on Protocols

In the below table we'll list the valid operations on resources that are executed by the Kafka API protocols.  
  
<table>  
<tr>  
<th>

Protocol (API key)
</th>  
<th>

Operation
</th>  
<th>

Resource
</th>  
<th>

Note
</th> </tr>  
<tr>  
<td>

PRODUCE (0)
</td>  
<td>

Write
</td>  
<td>

TransactionalId
</td>  
<td>

An transactional producer which has its transactional.id set requires this privilege.
</td> </tr>  
<tr>  
<td>

PRODUCE (0)
</td>  
<td>

IdempotentWrite
</td>  
<td>

Cluster
</td>  
<td>

An idempotent produce action requires this privilege.
</td> </tr>  
<tr>  
<td>

PRODUCE (0)
</td>  
<td>

Write
</td>  
<td>

Topic
</td>  
<td>

This applies to a normal produce action.
</td> </tr>  
<tr>  
<td>

FETCH (1)
</td>  
<td>

ClusterAction
</td>  
<td>

Cluster
</td>  
<td>

A follower must have ClusterAction on the Cluster resource in order to fetch partition data.
</td> </tr>  
<tr>  
<td>

FETCH (1)
</td>  
<td>

Read
</td>  
<td>

Topic
</td>  
<td>

Regular Kafka consumers need READ permission on each partition they are fetching.
</td> </tr>  
<tr>  
<td>

LIST_OFFSETS (2)
</td>  
<td>

Describe
</td>  
<td>

Topic
</td>  
<td>


</td> </tr>  
<tr>  
<td>

METADATA (3)
</td>  
<td>

Describe
</td>  
<td>

Topic
</td>  
<td>


</td> </tr>  
<tr>  
<td>

METADATA (3)
</td>  
<td>

Create
</td>  
<td>

Cluster
</td>  
<td>

If topic auto-creation is enabled, then the broker-side API will check for the existence of a Cluster level privilege. If it's found then it'll allow creating the topic, otherwise it'll iterate through the Topic level privileges (see the next one).
</td> </tr>  
<tr>  
<td>

METADATA (3)
</td>  
<td>

Create
</td>  
<td>

Topic
</td>  
<td>

This authorizes auto topic creation if enabled but the given user doesn't have a cluster level permission (above).
</td> </tr>  
<tr>  
<td>

LEADER_AND_ISR (4)
</td>  
<td>

ClusterAction
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

STOP_REPLICA (5)
</td>  
<td>

ClusterAction
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

UPDATE_METADATA (6)
</td>  
<td>

ClusterAction
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

CONTROLLED_SHUTDOWN (7)
</td>  
<td>

ClusterAction
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

OFFSET_COMMIT (8)
</td>  
<td>

Read
</td>  
<td>

Group
</td>  
<td>

An offset can only be committed if it's authorized to the given group and the topic too (see below). Group access is checked first, then Topic access.
</td> </tr>  
<tr>  
<td>

OFFSET_COMMIT (8)
</td>  
<td>

Read
</td>  
<td>

Topic
</td>  
<td>

Since offset commit is part of the consuming process, it needs privileges for the read action.
</td> </tr>  
<tr>  
<td>

OFFSET_FETCH (9)
</td>  
<td>

Describe
</td>  
<td>

Group
</td>  
<td>

Similarly to OFFSET_COMMIT, the application must have privileges on group and topic level too to be able to fetch. However in this case it requires describe access instead of read. Group access is checked first, then Topic access.
</td> </tr>  
<tr>  
<td>

OFFSET_FETCH (9)
</td>  
<td>

Describe
</td>  
<td>

Topic
</td>  
<td>


</td> </tr>  
<tr>  
<td>

FIND_COORDINATOR (10)
</td>  
<td>

Describe
</td>  
<td>

Group
</td>  
<td>

The FIND_COORDINATOR request can be of "Group" type in which case it is looking for consumergroup coordinators. This privilege would represent the Group mode.
</td> </tr>  
<tr>  
<td>

FIND_COORDINATOR (10)
</td>  
<td>

Describe
</td>  
<td>

TransactionalId
</td>  
<td>

This applies only on transactional producers and checked when a producer tries to find the transaction coordinator.
</td> </tr>  
<tr>  
<td>

JOIN_GROUP (11)
</td>  
<td>

Read
</td>  
<td>

Group
</td>  
<td>


</td> </tr>  
<tr>  
<td>

HEARTBEAT (12)
</td>  
<td>

Read
</td>  
<td>

Group
</td>  
<td>


</td> </tr>  
<tr>  
<td>

LEAVE_GROUP (13)
</td>  
<td>

Read
</td>  
<td>

Group
</td>  
<td>


</td> </tr>  
<tr>  
<td>

SYNC_GROUP (14)
</td>  
<td>

Read
</td>  
<td>

Group
</td>  
<td>


</td> </tr>  
<tr>  
<td>

DESCRIBE_GROUPS (15)
</td>  
<td>

Describe
</td>  
<td>

Group
</td>  
<td>


</td> </tr>  
<tr>  
<td>

LIST_GROUPS (16)
</td>  
<td>

Describe
</td>  
<td>

Cluster
</td>  
<td>

When the broker checks to authorize a list_groups request it first checks for this cluster level authorization. If none found then it proceeds to check the groups individually. This operation doesn't return CLUSTER_AUTHORIZATION_FAILED.
</td> </tr>  
<tr>  
<td>

LIST_GROUPS (16)
</td>  
<td>

Describe
</td>  
<td>

Group
</td>  
<td>

If none of the groups are authorized, then just an empty response will be sent back instead of an error. This operation doesn't return CLUSTER_AUTHORIZATION_FAILED. This is applicable from the 2.1 release.
</td> </tr>  
<tr>  
<td>

SASL_HANDSHAKE (17)
</td>  
<td>


</td>  
<td>


</td>  
<td>

The SASL handshake is part of the authentication process and therefore it's not possible to apply any kind of authorization here.
</td> </tr>  
<tr>  
<td>

API_VERSIONS (18)
</td>  
<td>


</td>  
<td>


</td>  
<td>

The API_VERSIONS request is part of the Kafka protocol handshake and happens on connection and before any authentication. Therefore it's not possible to control this with authorization.
</td> </tr>  
<tr>  
<td>

CREATE_TOPICS (19)
</td>  
<td>

Create
</td>  
<td>

Cluster
</td>  
<td>

If there is no cluster level authorization then it won't return CLUSTER_AUTHORIZATION_FAILED but fall back to use topic level, which is just below. That'll throw error if there is a problem.
</td> </tr>  
<tr>  
<td>

CREATE_TOPICS (19)
</td>  
<td>

Create
</td>  
<td>

Topic
</td>  
<td>

This is applicable from the 2.0 release.
</td> </tr>  
<tr>  
<td>

DELETE_TOPICS (20)
</td>  
<td>

Delete
</td>  
<td>

Topic
</td>  
<td>


</td> </tr>  
<tr>  
<td>

DELETE_RECORDS (21)
</td>  
<td>

Delete
</td>  
<td>

Topic
</td>  
<td>


</td> </tr>  
<tr>  
<td>

INIT_PRODUCER_ID (22)
</td>  
<td>

Write
</td>  
<td>

TransactionalId
</td>  
<td>


</td> </tr>  
<tr>  
<td>

INIT_PRODUCER_ID (22)
</td>  
<td>

IdempotentWrite
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

OFFSET_FOR_LEADER_EPOCH (23)
</td>  
<td>

ClusterAction
</td>  
<td>

Cluster
</td>  
<td>

If there is no cluster level privilege for this operation, then it'll check for topic level one.
</td> </tr>  
<tr>  
<td>

OFFSET_FOR_LEADER_EPOCH (23)
</td>  
<td>

Describe
</td>  
<td>

Topic
</td>  
<td>

This is applicable from the 2.1 release.
</td> </tr>  
<tr>  
<td>

ADD_PARTITIONS_TO_TXN (24)
</td>  
<td>

Write
</td>  
<td>

TransactionalId
</td>  
<td>

This API is only applicable to transactional requests. It first checks for the Write action on the TransactionalId resource, then it checks the Topic in subject (below).
</td> </tr>  
<tr>  
<td>

ADD_PARTITIONS_TO_TXN (24)
</td>  
<td>

Write
</td>  
<td>

Topic
</td>  
<td>


</td> </tr>  
<tr>  
<td>

ADD_OFFSETS_TO_TXN (25)
</td>  
<td>

Write
</td>  
<td>

TransactionalId
</td>  
<td>

Similarly to ADD_PARTITIONS_TO_TXN this is only applicable to transactional request. It first checks for Write action on the TransactionalId resource, then it checks whether it can Read on the given group (below).
</td> </tr>  
<tr>  
<td>

ADD_OFFSETS_TO_TXN (25)
</td>  
<td>

Read
</td>  
<td>

Group
</td>  
<td>


</td> </tr>  
<tr>  
<td>

END_TXN (26)
</td>  
<td>

Write
</td>  
<td>

TransactionalId
</td>  
<td>


</td> </tr>  
<tr>  
<td>

WRITE_TXN_MARKERS (27)
</td>  
<td>

Alter
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

WRITE_TXN_MARKERS (27)
</td>  
<td>

ClusterAction
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

TXN_OFFSET_COMMIT (28)
</td>  
<td>

Write
</td>  
<td>

TransactionalId
</td>  
<td>


</td> </tr>  
<tr>  
<td>

TXN_OFFSET_COMMIT (28)
</td>  
<td>

Read
</td>  
<td>

Group
</td>  
<td>


</td> </tr>  
<tr>  
<td>

TXN_OFFSET_COMMIT (28)
</td>  
<td>

Read
</td>  
<td>

Topic
</td>  
<td>


</td> </tr>  
<tr>  
<td>

DESCRIBE_ACLS (29)
</td>  
<td>

Describe
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

CREATE_ACLS (30)
</td>  
<td>

Alter
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

DELETE_ACLS (31)
</td>  
<td>

Alter
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

DESCRIBE_CONFIGS (32)
</td>  
<td>

DescribeConfigs
</td>  
<td>

Cluster
</td>  
<td>

If broker configs are requested, then the broker will check cluster level privileges.
</td> </tr>  
<tr>  
<td>

DESCRIBE_CONFIGS (32)
</td>  
<td>

DescribeConfigs
</td>  
<td>

Topic
</td>  
<td>

If topic configs are requested, then the broker will check topic level privileges.
</td> </tr>  
<tr>  
<td>

ALTER_CONFIGS (33)
</td>  
<td>

AlterConfigs
</td>  
<td>

Cluster
</td>  
<td>

If broker configs are altered, then the broker will check cluster level privileges.
</td> </tr>  
<tr>  
<td>

ALTER_CONFIGS (33)
</td>  
<td>

AlterConfigs
</td>  
<td>

Topic
</td>  
<td>

If topic configs are altered, then the broker will check topic level privileges.
</td> </tr>  
<tr>  
<td>

ALTER_REPLICA_LOG_DIRS (34)
</td>  
<td>

Alter
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

DESCRIBE_LOG_DIRS (35)
</td>  
<td>

Describe
</td>  
<td>

Cluster
</td>  
<td>

An empty response will be returned on authorization failure.
</td> </tr>  
<tr>  
<td>

SASL_AUTHENTICATE (36)
</td>  
<td>


</td>  
<td>


</td>  
<td>

SASL_AUTHENTICATE is part of the authentication process and therefore it's not possible to apply any kind of authorization here.
</td> </tr>  
<tr>  
<td>

CREATE_PARTITIONS (37)
</td>  
<td>

Alter
</td>  
<td>

Topic
</td>  
<td>


</td> </tr>  
<tr>  
<td>

CREATE_DELEGATION_TOKEN (38)
</td>  
<td>


</td>  
<td>


</td>  
<td>

Creating delegation tokens has special rules, for this please see the Authentication using Delegation Tokens section.
</td> </tr>  
<tr>  
<td>

CREATE_DELEGATION_TOKEN (38)
</td>  
<td>

CreateTokens
</td>  
<td>

User
</td>  
<td>

Allows creating delegation tokens for the User resource.
</td> </tr>  
<tr>  
<td>

RENEW_DELEGATION_TOKEN (39)
</td>  
<td>


</td>  
<td>


</td>  
<td>

Renewing delegation tokens has special rules, for this please see the Authentication using Delegation Tokens section.
</td> </tr>  
<tr>  
<td>

EXPIRE_DELEGATION_TOKEN (40)
</td>  
<td>


</td>  
<td>


</td>  
<td>

Expiring delegation tokens has special rules, for this please see the Authentication using Delegation Tokens section.
</td> </tr>  
<tr>  
<td>

DESCRIBE_DELEGATION_TOKEN (41)
</td>  
<td>

Describe
</td>  
<td>

DelegationToken
</td>  
<td>

Describing delegation tokens has special rules, for this please see the Authentication using Delegation Tokens section.
</td> </tr>  
<tr>  
<td>

DESCRIBE_DELEGATION_TOKEN (41)
</td>  
<td>

DescribeTokens
</td>  
<td>

User
</td>  
<td>

Allows describing delegation tokens of the User resource.
</td> </tr>  
<tr>  
<td>

DELETE_GROUPS (42)
</td>  
<td>

Delete
</td>  
<td>

Group
</td>  
<td>


</td> </tr>  
<tr>  
<td>

ELECT_PREFERRED_LEADERS (43)
</td>  
<td>

ClusterAction
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

INCREMENTAL_ALTER_CONFIGS (44)
</td>  
<td>

AlterConfigs
</td>  
<td>

Cluster
</td>  
<td>

If broker configs are altered, then the broker will check cluster level privileges.
</td> </tr>  
<tr>  
<td>

INCREMENTAL_ALTER_CONFIGS (44)
</td>  
<td>

AlterConfigs
</td>  
<td>

Topic
</td>  
<td>

If topic configs are altered, then the broker will check topic level privileges.
</td> </tr>  
<tr>  
<td>

ALTER_PARTITION_REASSIGNMENTS (45)
</td>  
<td>

Alter
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

LIST_PARTITION_REASSIGNMENTS (46)
</td>  
<td>

Describe
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

OFFSET_DELETE (47)
</td>  
<td>

Delete
</td>  
<td>

Group
</td>  
<td>


</td> </tr>  
<tr>  
<td>

OFFSET_DELETE (47)
</td>  
<td>

Read
</td>  
<td>

Topic
</td>  
<td>


</td> </tr>  
<tr>  
<td>

DESCRIBE_CLIENT_QUOTAS (48)
</td>  
<td>

DescribeConfigs
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

ALTER_CLIENT_QUOTAS (49)
</td>  
<td>

AlterConfigs
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

DESCRIBE_USER_SCRAM_CREDENTIALS (50)
</td>  
<td>

Describe
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

ALTER_USER_SCRAM_CREDENTIALS (51)
</td>  
<td>

Alter
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

VOTE (52)
</td>  
<td>

ClusterAction
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

BEGIN_QUORUM_EPOCH (53)
</td>  
<td>

ClusterAction
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

END_QUORUM_EPOCH (54)
</td>  
<td>

ClusterAction
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

DESCRIBE_QUORUM (55)
</td>  
<td>

Describe
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

ALTER_PARTITION (56)
</td>  
<td>

ClusterAction
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

UPDATE_FEATURES (57)
</td>  
<td>

Alter
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

ENVELOPE (58)
</td>  
<td>

ClusterAction
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

FETCH_SNAPSHOT (59)
</td>  
<td>

ClusterAction
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

DESCRIBE_CLUSTER (60)
</td>  
<td>

Describe
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

DESCRIBE_PRODUCERS (61)
</td>  
<td>

Read
</td>  
<td>

Topic
</td>  
<td>


</td> </tr>  
<tr>  
<td>

BROKER_REGISTRATION (62)
</td>  
<td>

ClusterAction
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

BROKER_HEARTBEAT (63)
</td>  
<td>

ClusterAction
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

UNREGISTER_BROKER (64)
</td>  
<td>

Alter
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

DESCRIBE_TRANSACTIONS (65)
</td>  
<td>

Describe
</td>  
<td>

TransactionalId
</td>  
<td>


</td> </tr>  
<tr>  
<td>

LIST_TRANSACTIONS (66)
</td>  
<td>

Describe
</td>  
<td>

TransactionalId
</td>  
<td>


</td> </tr>  
<tr>  
<td>

ALLOCATE_PRODUCER_IDS (67)
</td>  
<td>

ClusterAction
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

CONSUMER_GROUP_HEARTBEAT (68)
</td>  
<td>

Read
</td>  
<td>

Group
</td>  
<td>


</td> </tr>  
<tr>  
<td>

CONSUMER_GROUP_DESCRIBE (69)
</td>  
<td>

Read
</td>  
<td>

Group
</td>  
<td>


</td> </tr>  
<tr>  
<td>

CONTROLLER_REGISTRATION (70)
</td>  
<td>

ClusterAction
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

GET_TELEMETRY_SUBSCRIPTIONS (71)
</td>  
<td>


</td>  
<td>


</td>  
<td>

No authorization check is performed for this request.
</td> </tr>  
<tr>  
<td>

PUSH_TELEMETRY (72)
</td>  
<td>


</td>  
<td>


</td>  
<td>

No authorization check is performed for this request.
</td> </tr>  
<tr>  
<td>

ASSIGN_REPLICAS_TO_DIRS (73)
</td>  
<td>

ClusterAction
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

LIST_CLIENT_METRICS_RESOURCES (74)
</td>  
<td>

DescribeConfigs
</td>  
<td>

Cluster
</td>  
<td>


</td> </tr>  
<tr>  
<td>

DESCRIBE_TOPIC_PARTITIONS (75)
</td>  
<td>

Describe
</td>  
<td>

Topic
</td>  
<td>


</td> </tr> </table>
