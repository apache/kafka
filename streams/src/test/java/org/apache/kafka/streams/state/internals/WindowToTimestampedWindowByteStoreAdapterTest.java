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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;

import static org.apache.kafka.streams.state.TimestampedBytesStore.convertToTimestampedFormat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class WindowToTimestampedWindowByteStoreAdapterTest {

    @Mock
    private WindowStore<Bytes, byte[]> mockStore;

    @Test
    public void shouldPropagateReadOnlyToInnerStoreWithFormatConversion() {
        when(mockStore.persistent()).thenReturn(true);
        final WindowToTimestampedWindowByteStoreAdapter adapter =
            new WindowToTimestampedWindowByteStoreAdapter(mockStore);
        final ReadOnlyWindowStore<Bytes, byte[]> innerView = mock(ReadOnlyWindowStore.class);
        final Bytes key = new Bytes("k".getBytes(StandardCharsets.UTF_8));
        final byte[] committedPlain = "committed".getBytes(StandardCharsets.UTF_8);

        when(mockStore.readOnly(IsolationLevel.READ_COMMITTED)).thenReturn(innerView);
        when(innerView.fetch(key, 10L)).thenReturn(committedPlain);

        final ReadOnlyWindowStore<Bytes, byte[]> view = adapter.readOnly(IsolationLevel.READ_COMMITTED);

        assertNotSame(adapter, view);
        assertArrayEquals(convertToTimestampedFormat(committedPlain), view.fetch(key, 10L));
        verify(mockStore).readOnly(IsolationLevel.READ_COMMITTED);
    }
}
