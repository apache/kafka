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
package org.apache.kafka.streams.processor.api;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.stream.Collectors;

public class StreamsHeaders implements Headers {
    private final LinkedList<Header> headers;

    public static Headers emptyHeaders() {
        return new StreamsHeaders(new LinkedList<>());
    }

    public static Headers fromHeaders(final Headers headers) {
        if (headers instanceof StreamsHeaders) {
            return headers;
        } else {
            final LinkedList<Header> headerList = new LinkedList<>();
            for (final Header header : headers) {
                headerList.add(header);
            }
            return new StreamsHeaders(headerList);
        }
    }

    private StreamsHeaders(final LinkedList<Header> headers) {
        this.headers = headers;
    }

    @Override
    public Headers add(final Header header) {
        final LinkedList<Header> result = new LinkedList<>(this.headers);
        result.add(header);
        return new StreamsHeaders(result);
    }

    @Override
    public Headers add(final String key, final byte[] value) {
        return add(new RecordHeader(key, value));
    }

    @Override
    public Headers remove(final String key) {
        final LinkedList<Header> result = new LinkedList<>(this.headers);
        result.removeIf(header -> header.key().equals(key));
        return new StreamsHeaders(result);
    }

    @Override
    public Header lastHeader(final String key) {
        final Iterator<Header> descendingIterator = headers.descendingIterator();
        while (descendingIterator.hasNext()) {
            final Header next = descendingIterator.next();
            if (next.key().equals(key)) {
                return next;
            }
        }
        return null;
    }

    @Override
    public Iterable<Header> headers(final String key) {
        return headers.stream().filter(header -> header.key().equals(key)).collect(Collectors.toList());
    }

    @Override
    public Header[] toArray() {
        final Header[] arr = new Header[this.headers.size()];
        int i = 0;
        for (final Header header : headers) {
            arr[i++] = header;
        }
        return arr;
    }

    @Override
    public Iterator<Header> iterator() {
        return Collections.unmodifiableList(headers).iterator();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || !(o instanceof Headers)) return false;
        final Headers headers1 = (Headers) o;
        final Header[] a = toArray();
        final Header[] a2 = headers1.toArray();
        return Arrays.equals(a, a2);
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("Headers should not be used in hash collections.");
    }
}
