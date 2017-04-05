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
package org.apache.kafka.common.header;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Iterator;

import org.junit.Test;

public class RecordHeadersTest {

    @Test
    public void testAdd() {
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("key", "value".getBytes()));

        Header header = headers.iterator().next();
        assertEquals("key", header.key());
        assertEquals("value", new String(header.value()));


        headers.add(new RecordHeader("key2", "value2".getBytes()));

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
    public void testLastHeader() {
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("key", "value".getBytes()));
        headers.add(new RecordHeader("key", "value2".getBytes()));
        headers.add(new RecordHeader("key", "value3".getBytes()));

        assertEquals("value3", new String(headers.lastHeader("key").value()));
    }

    @Test
    public void testClose() throws IOException {
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("key", "value".getBytes()));
        Iterator<Header> headerIteratorBeforeClose = headers.iterator();
        headers.close();
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
        assertEquals("value", new String(keyHeaders.next().value()));
        assertEquals("value2", new String(keyHeaders.next().value()));
        assertFalse(keyHeaders.hasNext());

        keyHeaders = headers.headers("key1").iterator();
        assertEquals("key1value", new String(keyHeaders.next().value()));
        assertFalse(keyHeaders.hasNext());

        keyHeaders = headers.headers("key2").iterator();
        assertEquals("key2value", new String(keyHeaders.next().value()));
        assertFalse(keyHeaders.hasNext());

    }

    @Test
    public void testNew() throws IOException {
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("key", "value".getBytes()));
        headers.close();

        RecordHeaders newHeaders = new RecordHeaders(headers);
        newHeaders.add(new RecordHeader("key", "value2".getBytes()));

        //Ensure existing headers are not modified
        assertEquals("value", new String(headers.lastHeader("key").value()));
        assertEquals(1, getCount(headers));

        //Ensure new headers are modified
        assertEquals("value2", new String(newHeaders.lastHeader("key").value()));
        assertEquals(2, getCount(newHeaders));
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

}
