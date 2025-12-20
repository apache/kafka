---
title: Configuration Providers
description: Configuration Providers
weight: 11
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


Use configuration providers to load configuration data from external sources. This might include sensitive information, such as passwords, API keys, or other credentials. 

You have the following options:

  * Use a custom provider by creating a class implementing the [`ConfigProvider`](/39/javadoc/org/apache/kafka/common/config/provider/ConfigProvider.html) interface and packaging it into a JAR file. 
  * Use a built-in provider:
    * [`DirectoryConfigProvider`](/39/javadoc/org/apache/kafka/common/config/provider/DirectoryConfigProvider.html)
    * [`EnvVarConfigProvider`](/39/javadoc/org/apache/kafka/common/config/provider/EnvVarConfigProvider.html)
    * [`FileConfigProvider`](/39/javadoc/org/apache/kafka/common/config/provider/FileConfigProvider.html)



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
