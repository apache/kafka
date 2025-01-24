#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# $1 - The path of the GraalVM native-image. This binary is used to compile Java applications ahead-of-time into a standalone native binary.
# $2 - The path of the directory that contains the native-image configuration files.
# $3 - The path of the directory that contains the Apache Kafka libs.
# $4 - The path of the resulting Kafka native binary after the build process.

$1 --no-fallback \
  --enable-http \
  --enable-https \
  --allow-incomplete-classpath \
  --report-unsupported-elements-at-runtime \
  --install-exit-handlers \
  --enable-monitoring=jmxserver,jmxclient,heapdump,jvmstat \
  -H:+ReportExceptionStackTraces \
  -H:+EnableAllSecurityServices \
  -H:EnableURLProtocols=http,https \
  -H:AdditionalSecurityProviders=sun.security.jgss.SunProvider \
  -H:ReflectionConfigurationFiles="$2"/reflect-config.json \
  -H:JNIConfigurationFiles="$2"/jni-config.json \
  -H:ResourceConfigurationFiles="$2"/resource-config.json \
  -H:SerializationConfigurationFiles="$2"/serialization-config.json \
  -H:PredefinedClassesConfigurationFiles="$2"/predefined-classes-config.json \
  -H:DynamicProxyConfigurationFiles="$2"/proxy-config.json \
  --verbose \
  -march=compatibility \
  -cp "$3/*" kafka.docker.KafkaDockerWrapper \
  -o "$4"
