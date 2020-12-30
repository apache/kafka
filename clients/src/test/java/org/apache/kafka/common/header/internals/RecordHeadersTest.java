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
package org.apache.kafka.common.header.internals;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RecordHeadersTest {

    @Test
    public void testAdd() {
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("key", "value".getBytes()));

        Header header = headers.iterator().next();
        assertHeader("key", "value", header);

        headers.add(new RecordHeader("key2", "value2".getBytes()));

        assertHeader("key2", "value2", headers.lastHeader("key2"));
        assertEquals(2, getCount(headers));
    }

    @Test
    public void testRemove() {
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("key", "value".getBytes()));

        assertTrue(headers.iterator().hasNext());

        headers.remove("key");

        assertFalse(headers.iterator().hasNext());
    }

    @Test
    public void testAddRemoveInterleaved() {
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("key", "value".getBytes()));
        headers.add(new RecordHeader("key2", "value2".getBytes()));

        assertTrue(headers.iterator().hasNext());

        headers.remove("key");

        assertEquals(1, getCount(headers));

        headers.add(new RecordHeader("key3", "value3".getBytes()));
        
        assertNull(headers.lastHeader("key"));

        assertHeader("key2", "value2", headers.lastHeader("key2"));

        assertHeader("key3", "value3", headers.lastHeader("key3"));

        assertEquals(2, getCount(headers));

        headers.remove("key2");

        assertNull(headers.lastHeader("key"));

        assertNull(headers.lastHeader("key2"));

        assertHeader("key3", "value3", headers.lastHeader("key3"));

        assertEquals(1, getCount(headers));

        headers.add(new RecordHeader("key3", "value4".getBytes()));

        assertHeader("key3", "value4", headers.lastHeader("key3"));

        assertEquals(2, getCount(headers));

        headers.add(new RecordHeader("key", "valueNew".getBytes()));

        assertEquals(3, getCount(headers));


        assertHeader("key", "valueNew", headers.lastHeader("key"));

        headers.remove("key3");

        assertEquals(1, getCount(headers));

        assertNull(headers.lastHeader("key2"));

        headers.remove("key");

        assertFalse(headers.iterator().hasNext());
    }

    @Test
    public void testLastHeader() {
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("key", "value".getBytes()));
        headers.add(new RecordHeader("key", "value2".getBytes()));
        headers.add(new RecordHeader("key", "value3".getBytes()));

        assertHeader("key", "value3", headers.lastHeader("key"));
        assertEquals(3, getCount(headers));

    }

    @Test
    public void testReadOnly() throws IOException {
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("key", "value".getBytes()));
        Iterator<Header> headerIteratorBeforeClose = headers.iterator();
        headers.setReadOnly();
        try {
            headers.add(new RecordHeader("key", "value".getBytes()));
            fail("IllegalStateException expected as headers are closed");
        } catch (IllegalStateException ise) {
            //expected  
        }

        try {
            headers.remove("key");
            fail("IllegalStateException expected as headers are closed");
        } catch (IllegalStateException ise) {
            //expected  
        }

        try {
            Iterator<Header> headerIterator = headers.iterator();
            headerIterator.next();
            headerIterator.remove();
            fail("IllegalStateException expected as headers are closed");
        } catch (IllegalStateException ise) {
            //expected  
        }
        
        try {
            headerIteratorBeforeClose.next();
            headerIteratorBeforeClose.remove();
            fail("IllegalStateException expected as headers are closed");
        } catch (IllegalStateException ise) {
            //expected  
        }
    }

    @Test
    public void testHeaders() throws IOException {
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("key", "value".getBytes()));
        headers.add(new RecordHeader("key1", "key1value".getBytes()));
        headers.add(new RecordHeader("key", "value2".getBytes()));
        headers.add(new RecordHeader("key2", "key2value".getBytes()));


        Iterator<Header> keyHeaders = headers.headers("key").iterator();
        assertHeader("key", "value", keyHeaders.next());
        assertHeader("key", "value2", keyHeaders.next());
        assertFalse(keyHeaders.hasNext());

        keyHeaders = headers.headers("key1").iterator();
        assertHeader("key1", "key1value", keyHeaders.next());
        assertFalse(keyHeaders.hasNext());

        keyHeaders = headers.headers("key2").iterator();
        assertHeader("key2", "key2value", keyHeaders.next());
        assertFalse(keyHeaders.hasNext());

    }

    @Test
    public void testNew() throws IOException {
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("key", "value".getBytes()));
        headers.setReadOnly();

        RecordHeaders newHeaders = new RecordHeaders(headers);
        newHeaders.add(new RecordHeader("key", "value2".getBytes()));

        //Ensure existing headers are not modified
        assertHeader("key", "value", headers.lastHeader("key"));
        assertEquals(1, getCount(headers));

        //Ensure new headers are modified
        assertHeader("key", "value2", newHeaders.lastHeader("key"));
        assertEquals(2, getCount(newHeaders));
    }

    @Test
    public void shouldThrowNpeWhenAddingNullHeader() {
        final RecordHeaders recordHeaders = new RecordHeaders();
        assertThrows(NullPointerException.class, () -> recordHeaders.add(null));
    }

    @Test
    public void shouldThrowNpeWhenAddingCollectionWithNullHeader() {
        assertThrows(NullPointerException.class, () -> new RecordHeaders(new Header[1]));
    }

    private int getCount(Headers headers) {
        int count = 0;
        Iterator<Header> headerIterator = headers.iterator();
        while (headerIterator.hasNext()) {
            headerIterator.next();
            count++;
        }
        return count;
    }
    
    static void assertHeader(String key, String value, Header actual) {
        assertEquals(key, actual.key());
        assertTrue(Arrays.equals(value.getBytes(), actual.value()));
    }

}
