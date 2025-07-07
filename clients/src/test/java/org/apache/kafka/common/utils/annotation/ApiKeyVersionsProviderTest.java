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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ApiKeyVersionsProviderTest {
    @Test
    void testProvideArgumentsWithExplicitRange() {
        ApiKeyVersionsProvider provider = new ApiKeyVersionsProvider();

        ApiKeyVersionsSource mockSource = mock(ApiKeyVersionsSource.class);
        when(mockSource.apiKey()).thenReturn(ApiKeys.FETCH);
        when(mockSource.fromVersion()).thenReturn((short) 5);
        when(mockSource.toVersion()).thenReturn((short) 7);
        when(mockSource.enableUnstableLastVersion()).thenReturn(false);

        provider.accept(mockSource);

        List<Short> versions = provider.provideArguments(mock(ExtensionContext.class))
            .map(arg -> (Short) arg.get()[0])
            .collect(Collectors.toList());

        assertEquals(List.of((short) 5, (short) 6, (short) 7), versions);
    }

    @Test
    void testProvideArgumentsWithDefaultRange() {
        ApiKeyVersionsProvider provider = new ApiKeyVersionsProvider();

        ApiKeyVersionsSource mockSource = mock(ApiKeyVersionsSource.class);
        when(mockSource.apiKey()).thenReturn(ApiKeys.METADATA);
        when(mockSource.fromVersion()).thenReturn((short) -1);
        when(mockSource.toVersion()).thenReturn((short) -1);
        when(mockSource.enableUnstableLastVersion()).thenReturn(false);

        provider.accept(mockSource);

        short oldest = ApiKeys.METADATA.oldestVersion();
        short latest = ApiKeys.METADATA.latestVersion(false);

        List<Short> versions = provider.provideArguments(mock(ExtensionContext.class))
            .map(arg -> (Short) arg.get()[0])
            .collect(Collectors.toList());

        List<Short> expected = IntStream
            .rangeClosed(oldest, latest)
            .mapToObj(i -> (short) i)
            .collect(Collectors.toList());

        assertEquals(expected, versions);
    }

    @Test
    void testInvalidRangeThrowsExceptionFromGreaterThanTo() {
        ApiKeyVersionsProvider provider = new ApiKeyVersionsProvider();

        ApiKeyVersionsSource mockSource = mock(ApiKeyVersionsSource.class);
        when(mockSource.apiKey()).thenReturn(ApiKeys.FETCH);
        when(mockSource.fromVersion()).thenReturn((short) 10);
        when(mockSource.toVersion()).thenReturn((short) 5);
        when(mockSource.enableUnstableLastVersion()).thenReturn(false);

        assertThrows(IllegalArgumentException.class, () -> provider.accept(mockSource));
    }

    @Test
    void testFromVersionTooOldThrowsException() {
        ApiKeyVersionsProvider provider = new ApiKeyVersionsProvider();

        ApiKeyVersionsSource mockSource = mock(ApiKeyVersionsSource.class);
        when(mockSource.apiKey()).thenReturn(ApiKeys.FETCH);
        when(mockSource.fromVersion()).thenReturn((short) (ApiKeys.FETCH.oldestVersion() - 1));
        when(mockSource.toVersion()).thenReturn((short) 10);
        when(mockSource.enableUnstableLastVersion()).thenReturn(false);

        assertThrows(IllegalArgumentException.class, () -> provider.accept(mockSource));
    }

    @Test
    void testToVersionTooNewThrowsException() {
        ApiKeyVersionsProvider provider = new ApiKeyVersionsProvider();

        ApiKeyVersionsSource mockSource = mock(ApiKeyVersionsSource.class);
        when(mockSource.apiKey()).thenReturn(ApiKeys.FETCH);
        when(mockSource.fromVersion()).thenReturn((short) 0);
        when(mockSource.toVersion()).thenReturn((short) (ApiKeys.FETCH.latestVersion(true) + 1));
        when(mockSource.enableUnstableLastVersion()).thenReturn(true);

        assertThrows(IllegalArgumentException.class, () -> provider.accept(mockSource));
    }
}
