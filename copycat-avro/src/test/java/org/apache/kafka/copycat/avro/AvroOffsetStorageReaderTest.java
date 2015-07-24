/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.avro;
/*
import org.apache.kafka.copycat.data.Schema;
import org.apache.kafka.copycat.data.SchemaBuilder;
import org.apache.kafka.copycat.errors.CopycatRuntimeException;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
*/
public class AvroOffsetStorageReaderTest {

    /* FIXME
    private static final String namespace = "namespace";
    private OffsetBackingStore offsetBackingStore;
    private OffsetStorageReaderImpl offsetReader;

    // Most requests will make a request to the underlying storage and want to verify some info
    // about that request
    private Capture<Collection<ByteBuffer>> requestKeys;
    private Capture<Callback<Map<ByteBuffer, ByteBuffer>>> callback;
    private Future<Map<ByteBuffer, ByteBuffer>> storeFuture;


    // Cover a few different types to verify serialization works and that for keys we're allowed to
    // mix-and-match within a single request
    // TODO Add testing of complex types for stream IDs & offsets to these primitive types
    private static final List<Object> streamIds = Arrays.asList((Object) "stream1", (Object) 52);
    private static final Object nullStreamId = null;
    // Same as streamIds but with extra, unserializable entry
    private static final List<Object> streamIdsWithUnserializable
            = Arrays.asList((Object) "stream1", (Object) 52, new Date());
    private static final List<Object> longOffsets = Arrays.asList((Object) 12L, (Object) 24L);
    private static final List<Object> stringOffsets = Arrays.asList(
            (Object) "offset1", (Object) "offset2");
    private static final Schema longSchema = SchemaBuilder.builder().longType();
    private static final Schema stringSchema = SchemaBuilder.builder().stringType();
    // Serialized form of data to be returned by the storage layer
    private static final Map<ByteBuffer, ByteBuffer> longsSerialized
            = new HashMap<ByteBuffer, ByteBuffer>();
    private static final Map<ByteBuffer, ByteBuffer> stringsSerialized
            = new HashMap<ByteBuffer, ByteBuffer>();
    private static final Map<ByteBuffer, ByteBuffer> singleLongSerialized
            = new HashMap<ByteBuffer, ByteBuffer>();
    private static final Map<ByteBuffer, ByteBuffer> nullsSerialized
            = new HashMap<ByteBuffer, ByteBuffer>();
    private static final Map<ByteBuffer, ByteBuffer> longsSerializedWithInvalid
            = new HashMap<ByteBuffer, ByteBuffer>();

    static {
        for (int i = 0; i < longOffsets.size(); i++) {
            longsSerialized.put(
                    AvroData.serializeToAvro(streamIds.get(i)),
                    AvroData.serializeToAvro(longOffsets.get(i)));
        }

        for (int i = 0; i < stringOffsets.size(); i++) {
            stringsSerialized.put(
                    AvroData.serializeToAvro(streamIds.get(i)),
                    AvroData.serializeToAvro(stringOffsets.get(i)));
        }

        singleLongSerialized.put(
                AvroData.serializeToAvro(streamIds.get(0)),
                AvroData.serializeToAvro(longOffsets.get(0)));

        nullsSerialized.put(null, null);

        longsSerializedWithInvalid.put(
                AvroData.serializeToAvro(streamIds.get(0)),
                AvroData.serializeToAvro(longOffsets.get(0)));
        // You need to be careful about the bytes specified here because Avro's variable length
        // encoding allows some short byte sequences to be valid values for a long
        longsSerializedWithInvalid.put(
                AvroData.serializeToAvro(streamIds.get(1)),
                ByteBuffer.wrap(new byte[0]));

    }


    @Before
    public void setup() {
        offsetBackingStore = PowerMock.createMock(OffsetBackingStore.class);
        offsetReader = new OffsetStorageReaderImpl(offsetBackingStore, namespace);

        requestKeys = EasyMock.newCapture();
        callback = EasyMock.newCapture();
        storeFuture = PowerMock.createMock(Future.class);
    }

    @Test
    public void testGetOffsetsStringValues() throws Exception {
        expectStoreRequest(stringsSerialized);

        PowerMock.replayAll();

        Map<Object, Object> result = offsetReader.getOffsets(streamIds, stringSchema);
        assertEquals(2, requestKeys.getValue().size());
        assertEquals(mapFromLists(streamIds, stringOffsets), result);

        PowerMock.verifyAll();
    }

    @Test
    public void testGetOffsetsLongValues() throws Exception {
        expectStoreRequest(longsSerialized);

        PowerMock.replayAll();

        Map<Object, Object> result = offsetReader.getOffsets(streamIds, longSchema);
        assertEquals(2, requestKeys.getValue().size());
        assertEquals(mapFromLists(streamIds, longOffsets), result);

        PowerMock.verifyAll();
    }

    // getOffset() isn't too interesting since it's just a degenerate form of getOffsets(), so we
    // just do one simple validation
    @Test
    public void testGetOffset() throws Exception {
        expectStoreRequest(singleLongSerialized);

        PowerMock.replayAll();

        Object result = offsetReader.getOffset(streamIds.get(0), longSchema);
        assertEquals(1, requestKeys.getValue().size());
        assertEquals(longOffsets.get(0), result);

        PowerMock.verifyAll();
    }

    @Test
    public void testGetOffsetsNulls() throws Exception {
        // Should be able to store/load null values
        expectStoreRequest(nullsSerialized);

        PowerMock.replayAll();

        Object result = offsetReader.getOffset(null, longSchema);
        assertEquals(1, requestKeys.getValue().size());
        assertNull(result);

        PowerMock.verifyAll();
    }

    @Test
    public void testSerializationErrorReturnsOtherResults() throws Exception {
        expectStoreRequest(longsSerialized);

        PowerMock.replayAll();

        Map<Object, Object> result = offsetReader.getOffsets(streamIdsWithUnserializable, longSchema);
        assertEquals(2, requestKeys.getValue().size());
        assertEquals(mapFromLists(streamIds, longOffsets), result);

        PowerMock.verifyAll();
    }

    @Test(expected = CopycatRuntimeException.class)
    public void testStorageGetFailed() throws Exception {
        // backing store failed -> CopycatRuntimeException
        EasyMock.expect(offsetBackingStore.get(EasyMock.eq(namespace), EasyMock.capture(requestKeys),
                EasyMock.capture(callback)))
                .andReturn(storeFuture);
        EasyMock.expect(storeFuture.get())
                .andThrow(new RuntimeException("failed to get data from backing store"));

        PowerMock.replayAll();

        Map<Object, Object> result = offsetReader.getOffsets(streamIds, longSchema);
        assertEquals(2, requestKeys.getValue().size()); // throws
    }

    @Test
    public void testUndeserializeableData() throws Exception {
        // Should return whatever data it can, ignoring the unserializeable data
        expectStoreRequest(longsSerializedWithInvalid);

        PowerMock.replayAll();

        Map<Object, Object> result = offsetReader.getOffsets(streamIds, longSchema);
        assertEquals(2, requestKeys.getValue().size());
        assertEquals(mapFromLists(streamIds.subList(0, 1), longOffsets.subList(0, 1)), result);

        PowerMock.verifyAll();
    }


    private void expectStoreRequest(Map<ByteBuffer, ByteBuffer> result) throws Exception {
        EasyMock.expect(offsetBackingStore.get(EasyMock.eq(namespace), EasyMock.capture(requestKeys),
                EasyMock.capture(callback)))
                .andReturn(storeFuture);
        EasyMock.expect(storeFuture.get()).andReturn(result);
    }

    private Map<Object, Object> mapFromLists(List<Object> keys, List<Object> values) {
        Map<Object, Object> result = new HashMap<Object, Object>();
        for (int i = 0; i < keys.size(); i++) {
            result.put(keys.get(i), values.get(i));
        }
        return result;
    }
    */
}
