---
title: Configuration
description: 
weight: 1
tags: ['kafka', 'docs']
aliases: 
keywords: 
type: docs
---

Kafka uses key-value pairs in the [property file format](https://en.wikipedia.org/wiki/.properties) for configuration. These values can be supplied either from a file or programmatically. 

# Broker Configs

The essential configurations are the following: 

  * `node.id`
  * `log.dirs`
  * `process.roles`
  * `controller.quorum.bootstrap.servers` 
Topic-level configurations and defaults are discussed in more detail below. {{< include-html file="/static/40/generated/kafka_config.html" >}} 

More details about broker configuration can be found in the scala class `kafka.server.KafkaConfig`.

## Updating Broker Configs

From Kafka version 1.1 onwards, some of the broker configs can be updated without restarting the broker. See the `Dynamic Update Mode` column in Broker Configs for the update mode of each broker config. 

  * `read-only`: Requires a broker restart for update
  * `per-broker`: May be updated dynamically for each broker
  * `cluster-wide`: May be updated dynamically as a cluster-wide default. May also be updated as a per-broker value for testing.

To alter the current broker configs for broker id 0 (for example, the number of log cleaner threads): 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --alter --add-config log.cleaner.threads=2

To describe the current dynamic broker configs for broker id 0: 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --describe

To delete a config override and revert to the statically configured or default value for broker id 0 (for example, the number of log cleaner threads): 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --alter --delete-config log.cleaner.threads

Some configs may be configured as a cluster-wide default to maintain consistent values across the whole cluster. All brokers in the cluster will process the cluster default update. For example, to update log cleaner threads on all brokers: 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-default --alter --add-config log.cleaner.threads=2

To describe the currently configured dynamic cluster-wide default configs: 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-default --describe

All configs that are configurable at cluster level may also be configured at per-broker level (e.g. for testing). If a config value is defined at different levels, the following order of precedence is used: 

  * Dynamic per-broker config stored in the metadata log
  * Dynamic cluster-wide default config stored in the metadata log
  * Static broker config from `server.properties`
  * Kafka default, see broker configs



### Updating SSL Keystore of an Existing Listener

Brokers may be configured with SSL keystores with short validity periods to reduce the risk of compromised certificates. Keystores may be updated dynamically without restarting the broker. The config name must be prefixed with the listener prefix `listener.name.{listenerName}.` so that only the keystore config of a specific listener is updated. The following configs may be updated in a single alter request at per-broker level: 

  * `ssl.keystore.type`
  * `ssl.keystore.location`
  * `ssl.keystore.password`
  * `ssl.key.password`

If the listener is the inter-broker listener, the update is allowed only if the new keystore is trusted by the truststore configured for that listener. For other listeners, no trust validation is performed on the keystore by the broker. Certificates must be signed by the same certificate authority that signed the old certificate to avoid any client authentication failures. 

### Updating SSL Truststore of an Existing Listener

Broker truststores may be updated dynamically without restarting the broker to add or remove certificates. Updated truststore will be used to authenticate new client connections. The config name must be prefixed with the listener prefix `listener.name.{listenerName}.` so that only the truststore config of a specific listener is updated. The following configs may be updated in a single alter request at per-broker level: 

  * `ssl.truststore.type`
  * `ssl.truststore.location`
  * `ssl.truststore.password`

If the listener is the inter-broker listener, the update is allowed only if the existing keystore for that listener is trusted by the new truststore. For other listeners, no trust validation is performed by the broker before the update. Removal of CA certificates used to sign client certificates from the new truststore can lead to client authentication failures. 

### Updating Default Topic Configuration

Default topic configuration options used by brokers may be updated without broker restart. The configs are applied to topics without a topic config override for the equivalent per-topic config. One or more of these configs may be overridden at cluster-default level used by all brokers. 

  * `log.segment.bytes`
  * `log.roll.ms`
  * `log.roll.hours`
  * `log.roll.jitter.ms`
  * `log.roll.jitter.hours`
  * `log.index.size.max.bytes`
  * `log.flush.interval.messages`
  * `log.flush.interval.ms`
  * `log.retention.bytes`
  * `log.retention.ms`
  * `log.retention.minutes`
  * `log.retention.hours`
  * `log.index.interval.bytes`
  * `log.cleaner.delete.retention.ms`
  * `log.cleaner.min.compaction.lag.ms`
  * `log.cleaner.max.compaction.lag.ms`
  * `log.cleaner.min.cleanable.ratio`
  * `log.cleanup.policy`
  * `log.segment.delete.delay.ms`
  * `unclean.leader.election.enable`
  * `min.insync.replicas`
  * `max.message.bytes`
  * `compression.type`
  * `log.preallocate`
  * `log.message.timestamp.type`



### Updating Log Cleaner Configs

Log cleaner configs may be updated dynamically at cluster-default level used by all brokers. The changes take effect on the next iteration of log cleaning. One or more of these configs may be updated: 

  * `log.cleaner.threads`
  * `log.cleaner.io.max.bytes.per.second`
  * `log.cleaner.dedupe.buffer.size`
  * `log.cleaner.io.buffer.size`
  * `log.cleaner.io.buffer.load.factor`
  * `log.cleaner.backoff.ms`



### Updating Thread Configs

The size of various thread pools used by the broker may be updated dynamically at cluster-default level used by all brokers. Updates are restricted to the range `currentSize / 2` to `currentSize * 2` to ensure that config updates are handled gracefully. 

  * `num.network.threads`
  * `num.io.threads`
  * `num.replica.fetchers`
  * `num.recovery.threads.per.data.dir`
  * `log.cleaner.threads`
  * `background.threads`
  * `remote.log.reader.threads`
  * `remote.log.manager.copier.thread.pool.size`
  * `remote.log.manager.expiration.thread.pool.size`



### Updating ConnectionQuota Configs

The maximum number of connections allowed for a given IP/host by the broker may be updated dynamically at cluster-default level used by all brokers. The changes will apply for new connection creations and the existing connections count will be taken into account by the new limits. 

  * `max.connections.per.ip`
  * `max.connections.per.ip.overrides`



### Adding and Removing Listeners

Listeners may be added or removed dynamically. When a new listener is added, security configs of the listener must be provided as listener configs with the listener prefix `listener.name.{listenerName}.`. If the new listener uses SASL, the JAAS configuration of the listener must be provided using the JAAS configuration property `sasl.jaas.config` with the listener and mechanism prefix. See JAAS configuration for Kafka brokers for details.

In Kafka version 1.1.x, the listener used by the inter-broker listener may not be updated dynamically. To update the inter-broker listener to a new listener, the new listener may be added on all brokers without restarting the broker. A rolling restart is then required to update `inter.broker.listener.name`.

In addition to all the security configs of new listeners, the following configs may be updated dynamically at per-broker level: 

  * `listeners`
  * `advertised.listeners`
  * `listener.security.protocol.map`

Inter-broker listener must be configured using the static broker configuration `inter.broker.listener.name` or `security.inter.broker.protocol`. 

# Topic-Level Configs

Configurations pertinent to topics have both a server default as well an optional per-topic override. If no per-topic configuration is given the server default is used. The override can be set at topic creation time by giving one or more `--config` options. This example creates a topic named _my-topic_ with a custom max message size and flush rate: 
    
    
    $ bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic my-topic --partitions 1 \
      --replication-factor 1 --config max.message.bytes=64000 --config flush.messages=1

Overrides can also be changed or set later using the alter configs command. This example updates the max message size for _my-topic_ : 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name my-topic
      --alter --add-config max.message.bytes=128000

To check overrides set on the topic you can do 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name my-topic --describe

To remove an override you can do 
    
    
    $ bin/kafka-configs.sh --bootstrap-server localhost:9092  --entity-type topics --entity-name my-topic
      --alter --delete-config max.message.bytes

The following are the topic-level configurations. The server's default configuration for this property is given under the Server Default Property heading. A given server default config value only applies to a topic if it does not have an explicit topic config override. {{< include-html file="/static/40/generated/topic_config.html" >}} 

# Producer Configs

Below is the configuration of the producer: {{< include-html file="/static/40/generated/producer_config.html" >}} 

# Consumer Configs

Below is the configuration for the consumer: {{< include-html file="/static/40/generated/consumer_config.html" >}} 

# Kafka Connect Configs

Below is the configuration of the Kafka Connect framework. {{< include-html file="/static/40/generated/connect_config.html" >}} 

## Source Connector Configs

Below is the configuration of a source connector. {{< include-html file="/static/40/generated/source_connector_config.html" >}} 

## Sink Connector Configs

Below is the configuration of a sink connector. {{< include-html file="/static/40/generated/sink_connector_config.html" >}} 

# Kafka Streams Configs

Below is the configuration of the Kafka Streams client library. {{< include-html file="/static/40/generated/streams_config.html" >}} 

# Admin Configs

Below is the configuration of the Kafka Admin client library. {{< include-html file="/static/40/generated/admin_client_config.html" >}} 

# MirrorMaker Configs

Below is the configuration of the connectors that make up MirrorMaker 2. 

## MirrorMaker Common Configs

Below are the common configuration properties that apply to all three connectors. {{< include-html file="/static/40/generated/mirror_connector_config.html" >}} 

## MirrorMaker Source Configs

Below is the configuration of MirrorMaker 2 source connector for replicating topics. {{< include-html file="/static/40/generated/mirror_source_config.html" >}} 

## MirrorMaker Checkpoint Configs

Below is the configuration of MirrorMaker 2 checkpoint connector for emitting consumer offset checkpoints. {{< include-html file="/static/40/generated/mirror_checkpoint_config.html" >}} 

## MirrorMaker HeartBeat Configs

Below is the configuration of MirrorMaker 2 heartbeat connector for checking connectivity between connectors and clusters. {{< include-html file="/static/40/generated/mirror_heartbeat_config.html" >}} 

# System Properties

Kafka supports some configuration that can be enabled through Java system properties. System properties are usually set by passing the -D flag to the Java virtual machine in which Kafka components are running. Below are the supported system properties. 

  * #### org.apache.kafka.sasl.oauthbearer.allowed.urls

This system property is used to set the allowed URLs as SASL OAUTHBEARER token or jwks endpoints. This property accepts comma-separated list of URLs. By default the value is an empty list. 

If users want to enable some URLs, users need to explicitly set the system property like below. 
    
        -Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=https://www.example.com,file:///tmp/token

Since:| 4.0.0  
---|---  
Default Value:|   
  * #### org.apache.kafka.disallowed.login.modules

This system property is used to disable the problematic login modules usage in SASL JAAS configuration. This property accepts comma-separated list of loginModule names. By default **com.sun.security.auth.module.JndiLoginModule** loginModule is disabled. 

If users want to enable JndiLoginModule, users need to explicitly reset the system property like below. We advise the users to validate configurations and only allow trusted JNDI configurations. For more details [CVE-2023-25194](https://kafka.apache.org/cve-list#CVE-2023-25194). 
    
        -Dorg.apache.kafka.disallowed.login.modules=

To disable more loginModules, update the system property with comma-separated loginModule names. Make sure to explicitly add **JndiLoginModule** module name to the comma-separated list like below. 
    
        -Dorg.apache.kafka.disallowed.login.modules=com.sun.security.auth.module.JndiLoginModule,com.ibm.security.auth.module.LdapLoginModule,com.ibm.security.auth.module.Krb5LoginModule

Since:| 3.4.0  
---|---  
Default Value:| com.sun.security.auth.module.JndiLoginModule  
  * #### org.apache.kafka.automatic.config.providers

This system property controls the automatic loading of ConfigProvider implementations in Apache Kafka. ConfigProviders are used to dynamically supply configuration values from sources such as files, directories, or environment variables. This property accepts a comma-separated list of ConfigProvider names. By default, all built-in ConfigProviders are enabled, including **FileConfigProvider** , **DirectoryConfigProvider** , and **EnvVarConfigProvider**.

If users want to disable all automatic ConfigProviders, they need to explicitly set the system property as shown below. Disabling automatic ConfigProviders is recommended in environments where configuration data comes from untrusted sources or where increased security is required. For more details, see [CVE-2024-31141](https://kafka.apache.org/cve-list#CVE-2024-31141).
    
        -Dorg.apache.kafka.automatic.config.providers=none

To allow specific ConfigProviders, update the system property with a comma-separated list of fully qualified ConfigProvider class names. For example, to enable only the **EnvVarConfigProvider** , set the property as follows:
    
        -Dorg.apache.kafka.automatic.config.providers=org.apache.kafka.common.config.provider.EnvVarConfigProvider

To use multiple ConfigProviders, include their names in a comma-separated list as shown below:
    
        -Dorg.apache.kafka.automatic.config.providers=org.apache.kafka.common.config.provider.FileConfigProvider,org.apache.kafka.common.config.provider.EnvVarConfigProvider

Since:| 3.8.0  
---|---  
Default Value:| All built-in ConfigProviders are enabled  



# Tiered Storage Configs

Below are the configuration properties for Tiered Storage. {{< include-html file="/static/40/generated/remote_log_manager_config.html" >}} {{< include-html file="/static/40/generated/remote_log_metadata_manager_config.html" >}} 

# Configuration Providers

Use configuration providers to load configuration data from external sources. This might include sensitive information, such as passwords, API keys, or other credentials. 

You have the following options:

  * Use a custom provider by creating a class implementing the [`ConfigProvider`](/40/javadoc/org/apache/kafka/common/config/provider/ConfigProvider.html) interface and packaging it into a JAR file. 
  * Use a built-in provider:
    * [`DirectoryConfigProvider`](/40/javadoc/org/apache/kafka/common/config/provider/DirectoryConfigProvider.html)
    * [`EnvVarConfigProvider`](/40/javadoc/org/apache/kafka/common/config/provider/EnvVarConfigProvider.html)
    * [`FileConfigProvider`](/40/javadoc/org/apache/kafka/common/config/provider/FileConfigProvider.html)



To use a configuration provider, specify it in your configuration using the `config.providers` property. 

## Using Configuration Providers

Configuration providers allow you to pass parameters and retrieve configuration data from various sources.

To specify configuration providers, you use a comma-separated list of aliases and the fully-qualified class names that implement the configuration providers:
    
    
    config.providers=provider1,provider2
    config.providers.provider1.class=com.example.Provider1
    config.providers.provider2.class=com.example.Provider2

Each provider can have its own set of parameters, which are passed in a specific format:
    
    
    config.providers.<provider_alias>.param.<name>=<value>

The `ConfigProvider` interface serves as a base for all configuration providers. Custom implementations of this interface can be created to retrieve configuration data from various sources. You can package the implementation as a JAR file, add the JAR to your classpath, and reference the provider's class in your configuration.

**Example custom provider configuration**
    
    
    config.providers=customProvider
    config.providers.customProvider.class=com.example.customProvider
    config.providers.customProvider.param.param1=value1
    config.providers.customProvider.param.param2=value2

## DirectoryConfigProvider

The `DirectoryConfigProvider` retrieves configuration data from files stored in a specified directory.

Each file represents a key, and its content is the value. This provider is useful for loading multiple configuration files and for organizing configuration data into separate files.

To restrict the files that the `DirectoryConfigProvider` can access, use the `allowed.paths` parameter. This parameter accepts a comma-separated list of paths that the provider is allowed to access. If not set, all paths are allowed.

**Example`DirectoryConfigProvider` configuration**
    
    
    config.providers=dirProvider
    config.providers.dirProvider.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider
    config.providers.dirProvider.param.allowed.paths=/path/to/dir1,/path/to/dir2

To reference a value supplied by the `DirectoryConfigProvider`, use the correct placeholder syntax: 
    
    
    ${dirProvider:<path_to_file>:<file_name>}

## EnvVarConfigProvider

The `EnvVarConfigProvider` retrieves configuration data from environment variables.

No specific parameters are required, as it reads directly from the specified environment variables.

This provider is useful for configuring applications running in containers, for example, to load certificates or JAAS configuration from environment variables mapped from secrets.

To restrict which environment variables the `EnvVarConfigProvider` can access, use the `allowlist.pattern` parameter. This parameter accepts a regular expression that environment variable names must match to be used by the provider.

**Example`EnvVarConfigProvider` configuration**
    
    
    config.providers=envVarProvider
    config.providers.envVarProvider.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider
    config.providers.envVarProvider.param.allowlist.pattern=^MY_ENVAR1_.*

To reference a value supplied by the `EnvVarConfigProvider`, use the correct placeholder syntax: 
    
    
    ${envVarProvider:<enVar_name>}

## FileConfigProvider

The `FileConfigProvider` retrieves configuration data from a single properties file.

This provider is useful for loading configuration data from mounted files.

To restrict the file paths that the `FileConfigProvider` can access, use the `allowed.paths` parameter. This parameter accepts a comma-separated list of paths that the provider is allowed to access. If not set, all paths are allowed.

**Example`FileConfigProvider` configuration**
    
    
    config.providers=fileProvider
    config.providers.fileProvider.class=org.apache.kafka.common.config.provider.FileConfigProvider
    config.providers.fileProvider.param.allowed.paths=/path/to/config1,/path/to/config2

To reference a value supplied by the `FileConfigProvider`, use the correct placeholder syntax: 
    
    
    ${fileProvider:<path_and_filename>:<property>}

## Example: Referencing files

Here’s an example that uses a file configuration provider with Kafka Connect to provide authentication credentials to a database for a connector. 

First, create a `connector-credentials.properties` configuration file with the following credentials: 
    
    
    dbUsername=my-username
    dbPassword=my-password

Specify a `FileConfigProvider` in the Kafka Connect configuration: 

**Example Kafka Connect configuration with a`FileConfigProvider`**
    
    
    config.providers=fileProvider
    config.providers.fileProvider.class=org.apache.kafka.common.config.provider.FileConfigProvider

Next, reference the properties from the file in the connector configuration.

**Example connector configuration referencing file properties**
    
    
    database.user=${fileProvider:/path/to/connector-credentials.properties:dbUsername}
    database.password=${fileProvider:/path/to/connector-credentials.properties:dbPassword}

At runtime, the configuration provider reads and extracts the values from the properties file.
