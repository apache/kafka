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

import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Map;

import static org.apache.kafka.streams.state.internals.KeyValueStoreWrapper.PUT_RETURN_CODE_IS_LATEST;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class KeyValueStoreWrapperTest {

    private static final String STORE_NAME = "kvStore";
    private static final String KEY = "k";
    private static final ValueTimestampHeaders<String> VALUE_TIMESTAMP_HEADERS
        = ValueTimestampHeaders.make("v", 8L, null);

    @Mock
    private TimestampedKeyValueStoreWithHeaders<String, String> headersStore;
    @Mock
    private VersionedKeyValueStore<String, String> versionedStore;
    @Mock
    private ProcessorContext<?, ?> context;
    @Mock
    private Query<?> query;
    @Mock
    private PositionBound positionBound;
    @Mock
    private QueryConfig queryConfig;
    @Mock
    private QueryResult<?> result;
    @Mock
    private Position position;

    private KeyValueStoreWrapper<String, String> wrapper;

    @Test
    public void shouldThrowOnNonHeadersOrVersionedStore() {
        when(context.getStateStore(STORE_NAME)).thenReturn(mock(KeyValueStore.class));

        assertThrows(InvalidStateStoreException.class, () -> new KeyValueStoreWrapper<>(context, STORE_NAME));
    }

    @Test
    public void shouldGetFromHeadersStore() {
        givenWrapperWithHeadersStore();
        when(headersStore.get(KEY)).thenReturn(VALUE_TIMESTAMP_HEADERS);

        assertThat(wrapper.get(KEY), equalTo(VALUE_TIMESTAMP_HEADERS));
    }

    @Test
    public void shouldGetFromVersionedStore() {
        givenWrapperWithVersionedStore();
        when(versionedStore.get(KEY)).thenReturn(
            new VersionedRecord<>(
                VALUE_TIMESTAMP_HEADERS.value(),
                VALUE_TIMESTAMP_HEADERS.timestamp())
        );

        assertThat(wrapper.get(KEY), equalTo(VALUE_TIMESTAMP_HEADERS));
    }

    @Test
    public void shouldGetNullFromHeadersStore() {
        givenWrapperWithHeadersStore();
        when(headersStore.get(KEY)).thenReturn(null);

        assertThat(wrapper.get(KEY), nullValue());
    }

    @Test
    public void shouldGetNullFromVersionedStore() {
        givenWrapperWithVersionedStore();
        when(versionedStore.get(KEY)).thenReturn(null);

        assertThat(wrapper.get(KEY), nullValue());
    }

    @Test
    public void shouldPutToHeadersStore() {
        givenWrapperWithHeadersStore();

        final long putReturnCode = wrapper.put(KEY, VALUE_TIMESTAMP_HEADERS.value(), VALUE_TIMESTAMP_HEADERS.timestamp(), VALUE_TIMESTAMP_HEADERS.headers());

        assertThat(putReturnCode, equalTo(PUT_RETURN_CODE_IS_LATEST));
        verify(headersStore).put(KEY, VALUE_TIMESTAMP_HEADERS);
    }

    @Test
    public void shouldPutToVersionedStore() {
        givenWrapperWithVersionedStore();
        when(versionedStore.put(KEY, VALUE_TIMESTAMP_HEADERS.value(), VALUE_TIMESTAMP_HEADERS.timestamp())).thenReturn(12L);

        final long putReturnCode = wrapper.put(KEY, VALUE_TIMESTAMP_HEADERS.value(), VALUE_TIMESTAMP_HEADERS.timestamp(), VALUE_TIMESTAMP_HEADERS.headers());

        assertThat(putReturnCode, equalTo(12L));
    }

    @Test
    public void shouldPutNullToHeadersStore() {
        givenWrapperWithHeadersStore();

        final long putReturnCode = wrapper.put(KEY, null, VALUE_TIMESTAMP_HEADERS.timestamp(), VALUE_TIMESTAMP_HEADERS.headers());

        assertThat(putReturnCode, equalTo(PUT_RETURN_CODE_IS_LATEST));
        verify(headersStore).put(KEY, null);
    }

    @Test
    public void shouldPutNullToVersionedStore() {
        givenWrapperWithVersionedStore();
        when(versionedStore.put(KEY, null, VALUE_TIMESTAMP_HEADERS.timestamp())).thenReturn(12L);

        final long putReturnCode = wrapper.put(KEY, null, VALUE_TIMESTAMP_HEADERS.timestamp(), VALUE_TIMESTAMP_HEADERS.headers());

        assertThat(putReturnCode, equalTo(12L));
    }

    @Test
    public void shouldGetHeadersStore() {
        givenWrapperWithHeadersStore();

        assertThat(wrapper.store(), equalTo(headersStore));
    }

    @Test
    public void shouldGetVersionedStore() {
        givenWrapperWithVersionedStore();

        assertThat(wrapper.store(), equalTo(versionedStore));
    }

    @Test
    public void shouldGetNameForHeadersStore() {
        givenWrapperWithHeadersStore();
        when(headersStore.name()).thenReturn(STORE_NAME);

        assertThat(wrapper.name(), equalTo(STORE_NAME));
    }

    @Test
    public void shouldGetNameForVersionedStore() {
        givenWrapperWithVersionedStore();
        when(versionedStore.name()).thenReturn(STORE_NAME);

        assertThat(wrapper.name(), equalTo(STORE_NAME));
    }

    @Test
    public void shouldInitHeadersStore() {
        givenWrapperWithHeadersStore();
        final StateStoreContext mockContext = mock(StateStoreContext.class);

        wrapper.init(mockContext, wrapper);

        verify(headersStore).init(mockContext, wrapper);
    }

    @Test
    public void shouldInitVersionedStore() {
        givenWrapperWithVersionedStore();
        final StateStoreContext mockContext = mock(StateStoreContext.class);

        wrapper.init(mockContext, wrapper);

        verify(versionedStore).init(mockContext, wrapper);
    }

    @Test
    public void shouldCommitHeadersStore() {
        givenWrapperWithHeadersStore();

        wrapper.commit(Map.of());

        verify(headersStore).commit(Map.of());
    }

    @Test
    public void shouldCommitVersionedStore() {
        givenWrapperWithVersionedStore();

        wrapper.commit(Map.of());

        verify(versionedStore).commit(Map.of());
    }

    @Test
    public void shouldCloseHeadersStore() {
        givenWrapperWithHeadersStore();

        wrapper.close();

        verify(headersStore).close();
    }

    @Test
    public void shouldCloseVersionedStore() {
        givenWrapperWithVersionedStore();

        wrapper.close();

        verify(versionedStore).close();
    }

    @Test
    public void shouldReturnPersistentForHeadersStore() {
        givenWrapperWithHeadersStore();

        // test "persistent = true"
        when(headersStore.persistent()).thenReturn(true);
        assertThat(wrapper.persistent(), equalTo(true));

        // test "persistent = false"
        when(headersStore.persistent()).thenReturn(false);
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
    public void shouldReturnIsOpenForHeadersStore() {
        givenWrapperWithHeadersStore();

        // test "isOpen = true"
        when(headersStore.isOpen()).thenReturn(true);
        assertThat(wrapper.isOpen(), equalTo(true));

        // test "isOpen = false"
        when(headersStore.isOpen()).thenReturn(false);
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

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void shouldQueryHeadersStore() {
        givenWrapperWithHeadersStore();
        when(headersStore.query(query, positionBound, queryConfig)).thenReturn((QueryResult) result);

        assertThat(wrapper.query(query, positionBound, queryConfig), equalTo(result));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void shouldQueryVersionedStore() {
        givenWrapperWithVersionedStore();
        when(versionedStore.query(query, positionBound, queryConfig)).thenReturn((QueryResult) result);

        assertThat(wrapper.query(query, positionBound, queryConfig), equalTo(result));
    }

    @Test
    public void shouldGetPositionForHeadersStore() {
        givenWrapperWithHeadersStore();
        when(headersStore.getPosition()).thenReturn(position);

        assertThat(wrapper.getPosition(), equalTo(position));
    }

    @Test
    public void shouldGetPositionForVersionedStore() {
        givenWrapperWithVersionedStore();
        when(versionedStore.getPosition()).thenReturn(position);

        assertThat(wrapper.getPosition(), equalTo(position));
    }

    private void givenWrapperWithHeadersStore() {
        when(context.getStateStore(STORE_NAME)).thenReturn(headersStore);
        wrapper = new KeyValueStoreWrapper<>(context, STORE_NAME);
    }

    private void givenWrapperWithVersionedStore() {
        when(context.getStateStore(STORE_NAME)).thenReturn(versionedStore);
        wrapper = new KeyValueStoreWrapper<>(context, STORE_NAME);
    }
}