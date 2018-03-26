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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.AbstractIterator;

public class RecordHeaders implements Headers {
    
    private final List<Header> headers;
    private volatile boolean isReadOnly;

    public RecordHeaders() {
        this((Iterable<Header>) null);
    }

    public RecordHeaders(Header[] headers) {
        if (headers == null) {
            this.headers = new ArrayList<>();
        } else {
            this.headers = new ArrayList<>(Arrays.asList(headers));
        }
    }
    
    public RecordHeaders(Iterable<Header> headers) {
        //Use efficient copy constructor if possible, fallback to iteration otherwise
        if (headers == null) {
            this.headers = new ArrayList<>();
        } else if (headers instanceof RecordHeaders) {
            this.headers = new ArrayList<>(((RecordHeaders) headers).headers);
        } else if (headers instanceof Collection) {
            this.headers = new ArrayList<>((Collection<Header>) headers);
        } else {
            this.headers = new ArrayList<>();
            for (Header header : headers)
                this.headers.add(header);
        }
    }

    @Override
    public Headers add(Header header) throws IllegalStateException {
        canWrite();
        headers.add(header);
        return this;
    }

    @Override
    public Headers add(String key, byte[] value) throws IllegalStateException {
        return add(new RecordHeader(key, value));
    }

    @Override
    public Headers remove(String key) throws IllegalStateException {
        canWrite();
        checkKey(key);
        Iterator<Header> iterator = iterator();
        while (iterator.hasNext()) {
            if (iterator.next().key().equals(key)) {
                iterator.remove();
            }
        }
        return this;
    }

    @Override
    public Header lastHeader(String key) {
        checkKey(key);
        for (int i = headers.size() - 1; i >= 0; i--) {
            Header header = headers.get(i);
            if (header.key().equals(key)) {
                return header;
            }
        }
        return null;
    }

    @Override
    public Iterable<Header> headers(final String key) {
        checkKey(key);
        return new Iterable<Header>() {
            @Override
            public Iterator<Header> iterator() {
                return new FilterByKeyIterator(headers.iterator(), key);
            }
        };
    }

    @Override
    public Iterator<Header> iterator() {
        return closeAware(headers.iterator());
    }

    public void setReadOnly() {
        this.isReadOnly = true;
    }

    public Header[] toArray() {
        return headers.isEmpty() ? Record.EMPTY_HEADERS : headers.toArray(new Header[headers.size()]);
    }
    
    private void checkKey(String key) {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null.");
        }
    }
    
    private void canWrite() {
        if (isReadOnly) {
            throw new IllegalStateException("RecordHeaders has been closed.");
        }
    }

    private Iterator<Header> closeAware(final Iterator<Header> original) {
        return new Iterator<Header>() {
            @Override
            public boolean hasNext() {
                return original.hasNext();
            }

            public Header next() {
                return original.next();
            }

            @Override
            public void remove() {
                canWrite();
                original.remove();
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RecordHeaders headers1 = (RecordHeaders) o;

        return headers != null ? headers.equals(headers1.headers) : headers1.headers == null;
    }

    @Override
    public int hashCode() {
        return headers != null ? headers.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "RecordHeaders(" +
               "headers = " + headers +
               ", isReadOnly = " + isReadOnly +
               ')';
    }
    
    private static final class FilterByKeyIterator extends AbstractIterator<Header> {

        private final Iterator<Header> original;
        private final String key;

        private FilterByKeyIterator(Iterator<Header> original, String key) {
            this.original = original;
            this.key = key;
        }
        
        protected Header makeNext() {
            while (true) {
                if (original.hasNext()) {
                    Header header = original.next();
                    if (!header.key().equals(key)) {
                        continue;
                    }

                    return header;
                }
                return this.allDone();
            }
        }
    }
}
