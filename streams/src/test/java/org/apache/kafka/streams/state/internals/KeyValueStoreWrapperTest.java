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

import static org.apache.kafka.streams.state.internals.KeyValueStoreWrapper.PUT_RETURN_CODE_IS_LATEST;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class KeyValueStoreWrapperTest {

    private static final String STORE_NAME = "kvStore";
    private static final String KEY = "k";
    private static final ValueAndTimestamp<String> VALUE_AND_TIMESTAMP
        = ValueAndTimestamp.make("v", 8L);

    @Mock
    private TimestampedKeyValueStore<String, String> timestampedStore;
    @Mock
    private VersionedKeyValueStore<String, String> versionedStore;
    @Mock
    private ProcessorContext context;
    @Mock
    private Query query;
    @Mock
    private PositionBound positionBound;
    @Mock
    private QueryConfig queryConfig;
    @Mock
    private QueryResult result;
    @Mock
    private Position position;

    private KeyValueStoreWrapper<String, String> wrapper;

    @Test
    public void shouldThrowOnNonTimestampedOrVersionedStore() {
        when(context.getStateStore(STORE_NAME)).thenReturn(mock(KeyValueStore.class));

        assertThrows(InvalidStateStoreException.class, () -> new KeyValueStoreWrapper<>(context, STORE_NAME));
    }

    @Test
    public void shouldGetFromTimestampedStore() {
        givenWrapperWithTimestampedStore();
        when(timestampedStore.get(KEY)).thenReturn(VALUE_AND_TIMESTAMP);

        assertThat(wrapper.get(KEY), equalTo(VALUE_AND_TIMESTAMP));
    }

    @Test
    public void shouldGetFromVersionedStore() {
        givenWrapperWithVersionedStore();
        when(versionedStore.get(KEY)).thenReturn(
            new VersionedRecord<>(
                VALUE_AND_TIMESTAMP.value(),
                VALUE_AND_TIMESTAMP.timestamp())
        );

        assertThat(wrapper.get(KEY), equalTo(VALUE_AND_TIMESTAMP));
    }

    @Test
    public void shouldGetNullFromTimestampedStore() {
        givenWrapperWithTimestampedStore();
        when(timestampedStore.get(KEY)).thenReturn(null);

        assertThat(wrapper.get(KEY), nullValue());
    }

    @Test
    public void shouldGetNullFromVersionedStore() {
        givenWrapperWithVersionedStore();
        when(versionedStore.get(KEY)).thenReturn(null);

        assertThat(wrapper.get(KEY), nullValue());
    }

    @Test
    public void shouldPutToTimestampedStore() {
        givenWrapperWithTimestampedStore();

        final long putReturnCode = wrapper.put(KEY, VALUE_AND_TIMESTAMP.value(), VALUE_AND_TIMESTAMP.timestamp());

        assertThat(putReturnCode, equalTo(PUT_RETURN_CODE_IS_LATEST));
        verify(timestampedStore).put(KEY, VALUE_AND_TIMESTAMP);
    }

    @Test
    public void shouldPutToVersionedStore() {
        givenWrapperWithVersionedStore();
        when(versionedStore.put(KEY, VALUE_AND_TIMESTAMP.value(), VALUE_AND_TIMESTAMP.timestamp())).thenReturn(12L);

        final long putReturnCode = wrapper.put(KEY, VALUE_AND_TIMESTAMP.value(), VALUE_AND_TIMESTAMP.timestamp());

        assertThat(putReturnCode, equalTo(12L));
    }

    @Test
    public void shouldPutNullToTimestampedStore() {
        givenWrapperWithTimestampedStore();

        final long putReturnCode = wrapper.put(KEY, null, VALUE_AND_TIMESTAMP.timestamp());

        assertThat(putReturnCode, equalTo(PUT_RETURN_CODE_IS_LATEST));
        verify(timestampedStore).put(KEY, null);
    }

    @Test
    public void shouldPutNullToVersionedStore() {
        givenWrapperWithVersionedStore();
        when(versionedStore.put(KEY, null, VALUE_AND_TIMESTAMP.timestamp())).thenReturn(12L);

        final long putReturnCode = wrapper.put(KEY, null, VALUE_AND_TIMESTAMP.timestamp());

        assertThat(putReturnCode, equalTo(12L));
    }

    @Test
    public void shouldGetTimestampedStore() {
        givenWrapperWithTimestampedStore();

        assertThat(wrapper.getStore(), equalTo(timestampedStore));
    }

    @Test
    public void shouldGetVersionedStore() {
        givenWrapperWithVersionedStore();

        assertThat(wrapper.getStore(), equalTo(versionedStore));
    }

    @Test
    public void shouldGetNameForTimestampedStore() {
        givenWrapperWithTimestampedStore();
        when(timestampedStore.name()).thenReturn(STORE_NAME);

        assertThat(wrapper.name(), equalTo(STORE_NAME));
    }

    @Test
    public void shouldGetNameForVersionedStore() {
        givenWrapperWithVersionedStore();
        when(versionedStore.name()).thenReturn(STORE_NAME);

        assertThat(wrapper.name(), equalTo(STORE_NAME));
    }

    @Deprecated
    @Test
    public void shouldDeprecatedInitTimestampedStore() {
        givenWrapperWithTimestampedStore();
        final org.apache.kafka.streams.processor.ProcessorContext mockContext
            = mock(org.apache.kafka.streams.processor.ProcessorContext.class);

        wrapper.init(mockContext, wrapper);

        verify(timestampedStore).init(mockContext, wrapper);
    }

    @Deprecated
    @Test
    public void shouldDeprecatedInitVersionedStore() {
        givenWrapperWithVersionedStore();
        final org.apache.kafka.streams.processor.ProcessorContext mockContext
            = mock(org.apache.kafka.streams.processor.ProcessorContext.class);

        wrapper.init(mockContext, wrapper);

        verify(versionedStore).init(mockContext, wrapper);
    }

    @Test
    public void shouldInitTimestampedStore() {
        givenWrapperWithTimestampedStore();
        final StateStoreContext mockContext = mock(StateStoreContext.class);

        wrapper.init(mockContext, wrapper);

        verify(timestampedStore).init(mockContext, wrapper);
    }

    @Test
    public void shouldInitVersionedStore() {
        givenWrapperWithVersionedStore();
        final StateStoreContext mockContext = mock(StateStoreContext.class);

        wrapper.init(mockContext, wrapper);

        verify(versionedStore).init(mockContext, wrapper);
    }

    @Test
    public void shouldFlushTimestampedStore() {
        givenWrapperWithTimestampedStore();

        wrapper.flush();

        verify(timestampedStore).flush();
    }

    @Test
    public void shouldFlushVersionedStore() {
        givenWrapperWithVersionedStore();

        wrapper.flush();

        verify(versionedStore).flush();
    }

    @Test
    public void shouldCloseTimestampedStore() {
        givenWrapperWithTimestampedStore();

        wrapper.close();

        verify(timestampedStore).close();
    }

    @Test
    public void shouldCloseVersionedStore() {
        givenWrapperWithVersionedStore();

        wrapper.close();

        verify(versionedStore).close();
    }

    @Test
    public void shouldReturnPersistentForTimestampedStore() {
        givenWrapperWithTimestampedStore();

        // test "persistent = true"
        when(timestampedStore.persistent()).thenReturn(true);
        assertThat(wrapper.persistent(), equalTo(true));

        // test "persistent = false"
        when(timestampedStore.persistent()).thenReturn(false);
        assertThat(wrapper.persistent(), equalTo(false));
    }

    @Test
    public void shouldReturnPersistentForVersionedStore() {
        givenWrapperWithVersionedStore();

        // test "persistent = true"
        when(versionedStore.persistent()).thenReturn(true);
        assertThat(wrapper.persistent(), equalTo(true));

        // test "persistent = false"
        when(versionedStore.persistent()).thenReturn(false);
        assertThat(wrapper.persistent(), equalTo(false));
    }

    @Test
    public void shouldReturnIsOpenForTimestampedStore() {
        givenWrapperWithTimestampedStore();

        // test "isOpen = true"
        when(timestampedStore.isOpen()).thenReturn(true);
        assertThat(wrapper.isOpen(), equalTo(true));

        // test "isOpen = false"
        when(timestampedStore.isOpen()).thenReturn(false);
        assertThat(wrapper.isOpen(), equalTo(false));
    }

    @Test
    public void shouldReturnIsOpenForVersionedStore() {
        givenWrapperWithVersionedStore();

        // test "isOpen = true"
        when(versionedStore.isOpen()).thenReturn(true);
        assertThat(wrapper.isOpen(), equalTo(true));

        // test "isOpen = false"
        when(versionedStore.isOpen()).thenReturn(false);
        assertThat(wrapper.isOpen(), equalTo(false));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldQueryTimestampedStore() {
        givenWrapperWithTimestampedStore();
        when(timestampedStore.query(query, positionBound, queryConfig)).thenReturn(result);

        assertThat(wrapper.query(query, positionBound, queryConfig), equalTo(result));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldQueryVersionedStore() {
        givenWrapperWithVersionedStore();
        when(versionedStore.query(query, positionBound, queryConfig)).thenReturn(result);

        assertThat(wrapper.query(query, positionBound, queryConfig), equalTo(result));
    }

    @Test
    public void shouldGetPositionForTimestampedStore() {
        givenWrapperWithTimestampedStore();
        when(timestampedStore.getPosition()).thenReturn(position);

        assertThat(wrapper.getPosition(), equalTo(position));
    }

    @Test
    public void shouldGetPositionForVersionedStore() {
        givenWrapperWithVersionedStore();
        when(versionedStore.getPosition()).thenReturn(position);

        assertThat(wrapper.getPosition(), equalTo(position));
    }

    private void givenWrapperWithTimestampedStore() {
        when(context.getStateStore(STORE_NAME)).thenReturn(timestampedStore);
        wrapper = new KeyValueStoreWrapper<>(context, STORE_NAME);
    }

    private void givenWrapperWithVersionedStore() {
        when(context.getStateStore(STORE_NAME)).thenReturn(versionedStore);
        wrapper = new KeyValueStoreWrapper<>(context, STORE_NAME);
    }
}