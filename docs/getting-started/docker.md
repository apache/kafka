---
title: Docker
description: Learn how to pull and run the Kafka JVM Docker image and the experimental native Docker image.
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

```bash
$ docker pull apache/kafka:4.4.0
```

If you want to fetch the latest version of the Docker image use following command: 

```bash
$ docker pull apache/kafka:latest
```

To start the Kafka container using this Docker image with default configs and on default port 9092: 

```bash
$ docker run -p 9092:9092 apache/kafka:4.4.0
```

## GraalVM Based Native Apache Kafka Docker Image

Docker images for the GraalVM Based Native Apache Kafka can be found on [Docker Hub](https://hub.docker.com/r/apache/kafka-native) and are available from version 3.8.0.  
NOTE: This image is experimental and intended for local development and testing purposes only; it is not recommended for production use. 

Docker image can be pulled from Docker Hub using the following command: 

```bash
$ docker pull apache/kafka-native:4.4.0
```

If you want to fetch the latest version of the Docker image use following command: 

```bash
$ docker pull apache/kafka-native:latest
```

To start the Kafka container using this Docker image with default configs and on default port 9092: 

```bash
$ docker run -p 9092:9092 apache/kafka-native:4.4.0
```

### SASL and SSL

The native image supports SASL authentication since 4.4.0; earlier native images do not. See [Running in SASL mode](#running-in-sasl-mode) and [Running in SSL mode](#running-in-ssl-mode) below; both apply to the JVM and the native image.

## Usage guide

The Kafka server in the Docker container can be configured in the following ways:

- Default configs
- File input
- Environment variables

### Installation preparation

Note that the Docker version **must be >= 20.10.4**.

Earlier Docker versions may cause permission errors when running the Kafka container, as they do not correctly set directory permissions when creating container paths like `/opt/kafka/config`.

With an earlier version, you may encounter the following error during container startup:

```text
===> User
uid=1000(appuser) gid=1000(appuser) groups=1000(appuser)
===> Setting default values of environment variables if not already set.
===> Configuring …
Running in KRaft mode…
/opt/kafka/config/ file not writable
```

To avoid this, **please upgrade Docker to 20.10.4 or later**.

### Running on default configs

If no user provided configuration (file input or environment variables) is passed to the Docker container, the default KRaft configuration for a single combined-mode node will be used. This default configuration is packaged in the Kafka tarball.

### Using an input file for providing configs

- This method requires users to provide the path to a local folder which contains the Kafka property files and mount it to the Docker container using a Docker volume.
- It replaces the default KRaft configuration file present in the Docker container.
- The following command can be used to mount the folder containing the property files:

```bash
$ docker run --volume /path/to/property/folder:/mnt/shared/config -p 9092:9092 apache/kafka:latest
```

- Property files will only be read by the Docker container.

### Using environment variables

When using environment variables, you need to set all properties required to start the KRaft node. Therefore, the recommended way to use environment variables is via Docker Compose, which allows users to set all the properties that are needed. It is also possible to use the input file to have a common set of configurations, and then override specific node properties using environment variables.

- A Kafka property defined via an environment variable will override the value of that property defined in the user provided property file.
- If properties are provided via environment variables only, all required properties must be specified.
- The following rules must be used to construct the environment variable key name:
  - Replace `.` with `_`
  - Replace `_` with `__` (double underscore)
  - Replace `-` with `___` (triple underscore)
  - Prefix the result with `KAFKA_`
  - Examples:
    - For `abc.def`, use `KAFKA_ABC_DEF`
    - For `abc-def`, use `KAFKA_ABC___DEF`
    - For `abc_def`, use `KAFKA_ABC__DEF`
- To provide configs to log4j property files, the following points should be considered:
  - log4j properties provided via environment variables will be appended to the default properties file (log4j properties files bundled with Kafka).
  - `KAFKA_LOG4J_ROOT_LOGLEVEL` can be provided to set the value of `log4j.rootLogger` in `log4j2.yaml` and `tools-log4j2.yaml`.
  - log4j loggers can be added to `log4j2.yaml` by setting them in the `KAFKA_LOG4J_LOGGERS` environment variable as a single comma separated string.
    - Example: assuming that `KAFKA_LOG4J_LOGGERS='property1=value1,property2=value2'` is provided to the Docker container, `log4j.logger.property1=value1` and `log4j.logger.property2=value2` will be added to the `log4j2.yaml` file inside the Docker container.

### Running in SASL mode

- SASL mode requires a JAAS configuration file to be mounted into the Docker container and referenced via the `KAFKA_OPTS` environment variable.
- Mount the directory containing the JAAS config file to `/etc/kafka/secrets` in the Docker container.
- Set `KAFKA_OPTS` to `-Djava.security.auth.login.config=/etc/kafka/secrets/<jaas_config_filename>`.
- Set `KAFKA_SASL_ENABLED_MECHANISMS` to the desired SASL mechanism (e.g. `GSSAPI`, `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`).
- Ensure `KAFKA_ADVERTISED_LISTENERS` contains a `SASL_PLAINTEXT://` or `SASL_SSL://` listener.
- For inter-broker SASL communication, set `KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL` to the desired SASL *mechanism* (e.g. `PLAIN`, `SCRAM-SHA-256`). To choose between `SASL_PLAINTEXT` and `SASL_SSL` for inter-broker traffic, set `KAFKA_INTER_BROKER_LISTENER_NAME` to a listener whose security protocol is mapped accordingly in `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`.
- The Docker image `configure` script will log a warning if `KAFKA_OPTS` does not contain the `java.security.auth.login.config` property when SASL listeners are detected.
- For more details on SASL mechanisms and configuration, refer to the [Kafka SASL authentication documentation](https://kafka.apache.org/documentation/#security_sasl).

### Dynamic controller quorum

- By default the image formats storage for static voters (`KAFKA_CONTROLLER_QUORUM_VOTERS`).
- For a dynamic quorum (KIP-853), set `KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS` instead and pass the `kafka-storage format` option that sets up the quorum in `STORAGE_FORMAT_ARGS`:
  - `--initial-controllers` forms the whole quorum from the start. Pass the same value to every controller, e.g. `STORAGE_FORMAT_ARGS="--initial-controllers 1@controller-1:9093:<directory-id-1>,2@controller-2:9093:<directory-id-2>,3@controller-3:9093:<directory-id-3>"`. Generate each directory id once with `kafka-storage.sh random-uuid`; every controller formats its metadata log directory with its own id from the list.
  - Alternatively, start with a single voter and grow the quorum later: `--standalone` on the first controller, `--no-initial-controllers` on the others. They join as observers and are added with `kafka-metadata-quorum.sh add-controller`.
- `STORAGE_FORMAT_ARGS` is appended to the image's `kafka-storage format` command, so any of its options can be passed.
- Each controller must advertise its controller listener, e.g. `KAFKA_ADVERTISED_LISTENERS=CONTROLLER://controller-1:9093`. On a controller, `KAFKA_ADVERTISED_LISTENERS` may only list listeners from `KAFKA_CONTROLLER_LISTENER_NAMES`.

### Running in SSL mode

- The recommended way to run in SSL mode is by mounting secrets on `/etc/kafka/secrets` in the Docker container and providing the following configs through environment variables: `KAFKA_SSL_KEYSTORE_FILENAME`, `KAFKA_SSL_KEYSTORE_CREDENTIALS`, `KAFKA_SSL_KEY_CREDENTIALS`, `KAFKA_SSL_TRUSTSTORE_FILENAME` and `KAFKA_SSL_TRUSTSTORE_CREDENTIALS`. This lets the Docker image scripts extract passwords and populate the correct paths in `server.properties`.
- Please ensure appropriate `KAFKA_ADVERTISED_LISTENERS` are provided through environment variables to enable SSL mode in the Kafka server, i.e. it should contain an `SSL` listener.
- Alternatively, property file input can be used to provide SSL properties.
- Make sure you set the location of the truststore and keystore correctly when using file input. See the [file input example](https://github.com/apache/kafka/tree/trunk/docker/examples/docker-compose-files/single-node/file-input) for reference.
- Note that the `advertised.listeners` property needs to be provided along with the SSL properties in file input and cannot be provided through an environment variable separately.
- In conclusion, SSL properties with `advertised.listeners` should be treated as a group and provided in file input or environment variables in their entirety.
- In case SSL properties are provided both through file input and environment variables, environment variable properties will override the file input properties, as mentioned in [Using environment variables](#using-environment-variables).

### Examples

Docker Compose examples for single-node and multi-node (combined and isolated mode) clusters, covering plaintext, SSL, SASL and file input setups for both the JVM and the native image, are available in the [Kafka repository](https://github.com/apache/kafka/blob/trunk/docker/examples/README.md).
