---
title: Authorization and ACLs
description: Authorization and ACLs
weight: 5
tags: ['kafka', 'docs']
aliases: 
keywords: 
type: docs
---

# Authorization and ACLs

Kafka ships with a pluggable authorization framework, which is configured with the `authorizer.class.name` property in the server configuration. Configured implementations must extend `org.apache.kafka.server.authorizer.Authorizer`. Kafka provides a default implementation which store ACLs in the cluster metadata (KRaft metadata log). For KRaft clusters, use the following configuration on all nodes (brokers, controllers, or combined broker/controller nodes): 
    
    
    authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer

Kafka ACLs are defined in the general format of "Principal {P} is [Allowed|Denied] Operation {O} From Host {H} on any Resource {R} matching ResourcePattern {RP}". You can read more about the ACL structure in [KIP-11](https://cwiki.apache.org/confluence/display/KAFKA/KIP-11+-+Authorization+Interface) and resource patterns in [KIP-290](https://cwiki.apache.org/confluence/display/KAFKA/KIP-290%3A+Support+for+Prefixed+ACLs). In order to add, remove, or list ACLs, you can use the Kafka ACL CLI `kafka-acls.sh`. By default, if no ResourcePatterns match a specific Resource R, then R has no associated ACLs, and therefore no one other than super users is allowed to access R. If you want to change that behavior, you can include the following in server.properties. 
    
    
    allow.everyone.if.no.acl.found=true

One can also add super users in server.properties like the following (note that the delimiter is semicolon since SSL user names may contain comma). Default PrincipalType string "User" is case sensitive. 
    
    
    super.users=User:Bob;User:Alice

### KRaft Principal Forwarding

In KRaft clusters, admin requests such as `CreateTopics` and `DeleteTopics` are sent to the broker listeners by the client. The broker then forwards the request to the active controller through the first listener configured in `controller.listener.names`. Authorization of these requests is done on the controller node. This is achieved by way of an `Envelope` request which packages both the underlying request from the client as well as the client principal. When the controller receives the forwarded `Envelope` request from the broker, it first authorizes the `Envelope` request using the authenticated broker principal. Then it authorizes the underlying request using the forwarded principal.   
All of this implies that Kafka must understand how to serialize and deserialize the client principal. The authentication framework allows for customized principals by overriding the `principal.builder.class` configuration. In order for customized principals to work with KRaft, the configured class must implement `org.apache.kafka.common.security.auth.KafkaPrincipalSerde` so that Kafka knows how to serialize and deserialize the principals. The default implementation `org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder` uses the Kafka RPC format defined in the source code: `clients/src/main/resources/common/message/DefaultPrincipalData.json`. 

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

Option | Description | Default | Option type  
---|---|---|---  
\--add | Indicates to the script that user is trying to add an acl. |  | Action  
\--remove | Indicates to the script that user is trying to remove an acl. |  | Action  
\--list | Indicates to the script that user is trying to list acls. |  | Action  
\--bootstrap-server | A list of host/port pairs to use for establishing the connection to the Kafka cluster broker. Only one of --bootstrap-server or --bootstrap-controller option must be specified. |  | Configuration  
\--bootstrap-controller | A list of host/port pairs to use for establishing the connection to the Kafka cluster controller. Only one of --bootstrap-server or --bootstrap-controller option must be specified. |  | Configuration  
\--command-config | A property file containing configs to be passed to Admin Client. This option can only be used with --bootstrap-server option. |  | Configuration  
\--cluster | Indicates to the script that the user is trying to interact with acls on the singular cluster resource. |  | ResourcePattern  
\--topic [topic-name] | Indicates to the script that the user is trying to interact with acls on topic resource pattern(s). |  | ResourcePattern  
\--group [group-name] | Indicates to the script that the user is trying to interact with acls on consumer-group resource pattern(s) |  | ResourcePattern  
\--transactional-id [transactional-id] | The transactionalId to which ACLs should be added or removed. A value of * indicates the ACLs should apply to all transactionalIds. |  | ResourcePattern  
\--delegation-token [delegation-token] | Delegation token to which ACLs should be added or removed. A value of * indicates ACL should apply to all tokens. |  | ResourcePattern  
\--user-principal [user-principal] | A user resource to which ACLs should be added or removed. This is currently supported in relation with delegation tokens. A value of * indicates ACL should apply to all users. |  | ResourcePattern  
\--resource-pattern-type [pattern-type] | Indicates to the script the type of resource pattern, (for --add), or resource pattern filter, (for --list and --remove), the user wishes to use.  
When adding acls, this should be a specific pattern type, e.g. 'literal' or 'prefixed'.  
When listing or removing acls, a specific pattern type filter can be used to list or remove acls from a specific type of resource pattern, or the filter values of 'any' or 'match' can be used, where 'any' will match any pattern type, but will match the resource name exactly, and 'match' will perform pattern matching to list or remove all acls that affect the supplied resource(s).  
WARNING: 'match', when used in combination with the '--remove' switch, should be used with care.  | literal | Configuration  
\--allow-principal | Principal is in PrincipalType:name format that will be added to ACL with Allow permission. Default PrincipalType string "User" is case sensitive.   
You can specify multiple --allow-principal in a single command. |  | Principal  
\--deny-principal | Principal is in PrincipalType:name format that will be added to ACL with Deny permission. Default PrincipalType string "User" is case sensitive.   
You can specify multiple --deny-principal in a single command. |  | Principal  
\--principal | Principal is in PrincipalType:name format that will be used along with --list option. Default PrincipalType string "User" is case sensitive. This will list the ACLs for the specified principal.   
You can specify multiple --principal in a single command. |  | Principal  
\--allow-host | IP address from which principals listed in --allow-principal will have access. |  if --allow-principal is specified defaults to * which translates to "all hosts" | Host  
\--deny-host | IP address from which principals listed in --deny-principal will be denied access. | if --deny-principal is specified defaults to * which translates to "all hosts" | Host  
\--operation | Operation that will be allowed or denied.  
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

| All | Operation  
\--producer |  Convenience option to add/remove acls for producer role. This will generate acls that allows WRITE, DESCRIBE and CREATE on topic. |  | Convenience  
\--consumer |  Convenience option to add/remove acls for consumer role. This will generate acls that allows READ, DESCRIBE on topic and READ on consumer-group. |  | Convenience  
\--idempotent | Enable idempotence for the producer. This should be used in combination with the --producer option.  
Note that idempotence is enabled automatically if the producer is authorized to a particular transactional-id.  |  | Convenience  
\--force |  Convenience option to assume yes to all queries and do not prompt. |  | Convenience  
  
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

Protocol (API key) | Operation | Resource | Note  
---|---|---|---  
PRODUCE (0) | Write | TransactionalId | A transactional producer which has its transactional.id set requires this privilege.  
PRODUCE (0) | IdempotentWrite | Cluster | An idempotent produce action requires this privilege.  
PRODUCE (0) | Write | Topic | This applies to a normal produce action.  
FETCH (1) | ClusterAction | Cluster | A follower must have ClusterAction on the Cluster resource in order to fetch partition data.  
FETCH (1) | Read | Topic | Regular Kafka consumers need READ permission on each partition they are fetching.  
LIST_OFFSETS (2) | Describe | Topic |   
METADATA (3) | Describe | Topic |   
METADATA (3) | Create | Cluster | If topic auto-creation is enabled, then the broker-side API will check for the existence of a Cluster level privilege. If it's found then it'll allow creating the topic, otherwise it'll iterate through the Topic level privileges (see the next one).  
METADATA (3) | Create | Topic | This authorizes auto topic creation if enabled but the given user doesn't have a cluster level permission (above).  
LEADER_AND_ISR (4) | ClusterAction | Cluster |   
STOP_REPLICA (5) | ClusterAction | Cluster |   
UPDATE_METADATA (6) | ClusterAction | Cluster |   
CONTROLLED_SHUTDOWN (7) | ClusterAction | Cluster |   
OFFSET_COMMIT (8) | Read | Group | An offset can only be committed if it's authorized to the given group and the topic too (see below). Group access is checked first, then Topic access.  
OFFSET_COMMIT (8) | Read | Topic | Since offset commit is part of the consuming process, it needs privileges for the read action.  
OFFSET_FETCH (9) | Describe | Group | Similarly to OFFSET_COMMIT, the application must have privileges on group and topic level too to be able to fetch. However in this case it requires describe access instead of read. Group access is checked first, then Topic access.  
OFFSET_FETCH (9) | Describe | Topic |   
FIND_COORDINATOR (10) | Describe | Group | The FIND_COORDINATOR request can be of "Group" type in which case it is looking for consumergroup coordinators. This privilege would represent the Group mode.  
FIND_COORDINATOR (10) | Describe | TransactionalId | This applies only on transactional producers and checked when a producer tries to find the transaction coordinator.  
JOIN_GROUP (11) | Read | Group |   
HEARTBEAT (12) | Read | Group |   
LEAVE_GROUP (13) | Read | Group |   
SYNC_GROUP (14) | Read | Group |   
DESCRIBE_GROUPS (15) | Describe | Group |   
LIST_GROUPS (16) | Describe | Cluster | When the broker checks to authorize a list_groups request it first checks for this cluster level authorization. If none found then it proceeds to check the groups individually. This operation doesn't return CLUSTER_AUTHORIZATION_FAILED.  
LIST_GROUPS (16) | Describe | Group | If none of the groups are authorized, then just an empty response will be sent back instead of an error. This operation doesn't return CLUSTER_AUTHORIZATION_FAILED. This is applicable from the 2.1 release.  
SASL_HANDSHAKE (17) |  |  | The SASL handshake is part of the authentication process and therefore it's not possible to apply any kind of authorization here.  
API_VERSIONS (18) |  |  | The API_VERSIONS request is part of the Kafka protocol handshake and happens on connection and before any authentication. Therefore it's not possible to control this with authorization.  
CREATE_TOPICS (19) | Create | Cluster | If there is no cluster level authorization then it won't return CLUSTER_AUTHORIZATION_FAILED but fall back to use topic level, which is just below. That'll throw error if there is a problem.  
CREATE_TOPICS (19) | Create | Topic | This is applicable from the 2.0 release.  
DELETE_TOPICS (20) | Delete | Topic |   
DELETE_RECORDS (21) | Delete | Topic |   
INIT_PRODUCER_ID (22) | Write | TransactionalId |   
INIT_PRODUCER_ID (22) | IdempotentWrite | Cluster |   
OFFSET_FOR_LEADER_EPOCH (23) | ClusterAction | Cluster | If there is no cluster level privilege for this operation, then it'll check for topic level one.  
OFFSET_FOR_LEADER_EPOCH (23) | Describe | Topic | This is applicable from the 2.1 release.  
ADD_PARTITIONS_TO_TXN (24) | Write | TransactionalId | This API is only applicable to transactional requests. It first checks for the Write action on the TransactionalId resource, then it checks the Topic in subject (below).  
ADD_PARTITIONS_TO_TXN (24) | Write | Topic |   
ADD_OFFSETS_TO_TXN (25) | Write | TransactionalId | Similarly to ADD_PARTITIONS_TO_TXN this is only applicable to transactional request. It first checks for Write action on the TransactionalId resource, then it checks whether it can Read on the given group (below).  
ADD_OFFSETS_TO_TXN (25) | Read | Group |   
END_TXN (26) | Write | TransactionalId |   
WRITE_TXN_MARKERS (27) | Alter | Cluster |   
WRITE_TXN_MARKERS (27) | ClusterAction | Cluster |   
TXN_OFFSET_COMMIT (28) | Write | TransactionalId |   
TXN_OFFSET_COMMIT (28) | Read | Group |   
TXN_OFFSET_COMMIT (28) | Read | Topic |   
DESCRIBE_ACLS (29) | Describe | Cluster |   
CREATE_ACLS (30) | Alter | Cluster |   
DELETE_ACLS (31) | Alter | Cluster |   
DESCRIBE_CONFIGS (32) | DescribeConfigs | Cluster | If broker configs are requested, then the broker will check cluster level privileges.  
DESCRIBE_CONFIGS (32) | DescribeConfigs | Topic | If topic configs are requested, then the broker will check topic level privileges.  
ALTER_CONFIGS (33) | AlterConfigs | Cluster | If broker configs are altered, then the broker will check cluster level privileges.  
ALTER_CONFIGS (33) | AlterConfigs | Topic | If topic configs are altered, then the broker will check topic level privileges.  
ALTER_REPLICA_LOG_DIRS (34) | Alter | Cluster |   
DESCRIBE_LOG_DIRS (35) | Describe | Cluster | An empty response will be returned on authorization failure.  
SASL_AUTHENTICATE (36) |  |  | SASL_AUTHENTICATE is part of the authentication process and therefore it's not possible to apply any kind of authorization here.  
CREATE_PARTITIONS (37) | Alter | Topic |   
CREATE_DELEGATION_TOKEN (38) |  |  | Creating delegation tokens has special rules, for this please see the Authentication using Delegation Tokens section.  
CREATE_DELEGATION_TOKEN (38) | CreateTokens | User | Allows creating delegation tokens for the User resource.  
RENEW_DELEGATION_TOKEN (39) |  |  | Renewing delegation tokens has special rules, for this please see the Authentication using Delegation Tokens section.  
EXPIRE_DELEGATION_TOKEN (40) |  |  | Expiring delegation tokens has special rules, for this please see the Authentication using Delegation Tokens section.  
DESCRIBE_DELEGATION_TOKEN (41) | Describe | DelegationToken | Describing delegation tokens has special rules, for this please see the Authentication using Delegation Tokens section.  
DESCRIBE_DELEGATION_TOKEN (41) | DescribeTokens | User | Allows describing delegation tokens of the User resource.  
DELETE_GROUPS (42) | Delete | Group |   
ELECT_PREFERRED_LEADERS (43) | ClusterAction | Cluster |   
INCREMENTAL_ALTER_CONFIGS (44) | AlterConfigs | Cluster | If broker configs are altered, then the broker will check cluster level privileges.  
INCREMENTAL_ALTER_CONFIGS (44) | AlterConfigs | Topic | If topic configs are altered, then the broker will check topic level privileges.  
ALTER_PARTITION_REASSIGNMENTS (45) | Alter | Cluster |   
LIST_PARTITION_REASSIGNMENTS (46) | Describe | Cluster |   
OFFSET_DELETE (47) | Delete | Group |   
OFFSET_DELETE (47) | Read | Topic |   
DESCRIBE_CLIENT_QUOTAS (48) | DescribeConfigs | Cluster |   
ALTER_CLIENT_QUOTAS (49) | AlterConfigs | Cluster |   
DESCRIBE_USER_SCRAM_CREDENTIALS (50) | Describe | Cluster |   
ALTER_USER_SCRAM_CREDENTIALS (51) | Alter | Cluster |   
VOTE (52) | ClusterAction | Cluster |   
BEGIN_QUORUM_EPOCH (53) | ClusterAction | Cluster |   
END_QUORUM_EPOCH (54) | ClusterAction | Cluster |   
DESCRIBE_QUORUM (55) | Describe | Cluster |   
ALTER_PARTITION (56) | ClusterAction | Cluster |   
UPDATE_FEATURES (57) | Alter | Cluster |   
ENVELOPE (58) | ClusterAction | Cluster |   
FETCH_SNAPSHOT (59) | ClusterAction | Cluster |   
DESCRIBE_CLUSTER (60) | Describe | Cluster |   
DESCRIBE_PRODUCERS (61) | Read | Topic |   
BROKER_REGISTRATION (62) | ClusterAction | Cluster |   
BROKER_HEARTBEAT (63) | ClusterAction | Cluster |   
UNREGISTER_BROKER (64) | Alter | Cluster |   
DESCRIBE_TRANSACTIONS (65) | Describe | TransactionalId |   
LIST_TRANSACTIONS (66) | Describe | TransactionalId |   
ALLOCATE_PRODUCER_IDS (67) | ClusterAction | Cluster |   
CONSUMER_GROUP_HEARTBEAT (68) | Read | Group |   
CONSUMER_GROUP_DESCRIBE (69) | Read | Group |   
CONTROLLER_REGISTRATION (70) | ClusterAction | Cluster |   
GET_TELEMETRY_SUBSCRIPTIONS (71) |  |  | No authorization check is performed for this request.  
PUSH_TELEMETRY (72) |  |  | No authorization check is performed for this request.  
ASSIGN_REPLICAS_TO_DIRS (73) | ClusterAction | Cluster |   
LIST_CLIENT_METRICS_RESOURCES (74) | DescribeConfigs | Cluster |   
DESCRIBE_TOPIC_PARTITIONS (75) | Describe | Topic |   
SHARE_GROUP_HEARTBEAT (76) | Read | Group |   
SHARE_GROUP_DESCRIBE (77) | Describe | Group |   
SHARE_FETCH (78) | Read | Group |   
SHARE_FETCH (78) | Read | Topic |   
SHARE_ACKNOWLEDGE (79) | Read | Group |   
SHARE_ACKNOWLEDGE (79) | Read | Topic |   
INITIALIZE_SHARE_GROUP_STATE (83) | ClusterAction | Cluster |   
READ_SHARE_GROUP_STATE (84) | ClusterAction | Cluster |   
WRITE_SHARE_GROUP_STATE (85) | ClusterAction | Cluster |   
DELETE_SHARE_GROUP_STATE (86) | ClusterAction | Cluster |   
READ_SHARE_GROUP_STATE_SUMMARY (87) | ClusterAction | Cluster |   
  