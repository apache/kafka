/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.utils.annotation;

import org.apache.kafka.common.protocol.ApiKeys;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.support.AnnotationConsumer;

import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ApiKeyVersionsProvider implements ArgumentsProvider, AnnotationConsumer<ApiKeyVersionsSource> {
    private ApiKeys apiKey;
    private short fromVersion;
    private short toVersion;

    public void accept(ApiKeyVersionsSource source) {
        apiKey = source.apiKey();

        short oldestVersion = apiKey.oldestVersion();
        short latestVersion = apiKey.latestVersion(source.enableUnstableLastVersion());

        fromVersion = source.fromVersion() == -1 ? oldestVersion : source.fromVersion();
        toVersion = source.toVersion() == -1 ? latestVersion : source.toVersion();

        if (fromVersion > toVersion) {
            throw new IllegalArgumentException(String.format("The fromVersion %s is larger than the toVersion %s",
                fromVersion, toVersion));
        }

        if (fromVersion < oldestVersion) {
            throw new IllegalArgumentException(String.format("The fromVersion %s is older than the oldest version %s",
                fromVersion, oldestVersion));
        }

        if (toVersion > latestVersion) {
            throw new IllegalArgumentException(String.format("The toVersion %s is newer than the latest version %s",
                toVersion, latestVersion));
        }
    }

    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
        return IntStream.rangeClosed(fromVersion, toVersion).mapToObj(i -> Arguments.of((short) i));
    }
}
