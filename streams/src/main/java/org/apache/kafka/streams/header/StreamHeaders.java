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
package org.apache.kafka.streams.header;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Utils;

public class StreamHeaders implements Headers {

    private static final int EMPTY_HASH = Objects.hash(new LinkedList<>());

    private LinkedList<Header> headers;

    public StreamHeaders() {
    }

    public StreamHeaders(final Iterable<Header> original) {
        if (original == null) {
            return;
        }
        if (original instanceof StreamHeaders) {
            final StreamHeaders originalHeaders = (StreamHeaders) original;
            if (!originalHeaders.isEmpty()) {
                headers = new LinkedList<>(originalHeaders.headers);
            }
        } else {
            headers = new LinkedList<>();
            for (final Header header : original) {
                Objects.requireNonNull(header, "Unable to add a null header.");
                headers.add(header);
            }
        }
    }


    public static Headers wrap(final org.apache.kafka.common.header.Headers originals) {
        final LinkedList<Header> headers = new LinkedList<>();
        originals.forEach(header -> headers.add(StreamHeader.wrap(header)));
        return new StreamHeaders(headers);
    }

    public static Headers wrap(final org.apache.kafka.common.header.Header[] originals) {
        final LinkedList<Header> headers = new LinkedList<>();
        for (final org.apache.kafka.common.header.Header header : originals) {
            headers.add(StreamHeader.wrap(header));
        }
        return new StreamHeaders(headers);
    }

    @Override
    public int size() {
        return headers == null ? 0 : headers.size();
    }

    @Override
    public boolean isEmpty() {
        return headers == null || headers.isEmpty();
    }

    @Override
    public Headers clear() {
        if (headers != null) {
            headers.clear();
        }
        return this;
    }

    @Override
    public Headers add(final Header header) {
        Objects.requireNonNull(header, "Unable to add a null header.");
        if (headers == null) {
            headers = new LinkedList<>();
        }
        headers.add(header);
        return this;
    }

    @Override
    public Headers add(final String key, final byte[] value) {
        Objects.requireNonNull(key, "Unable to add a null header key.");
        if (headers == null) {
            headers = new LinkedList<>();
        }
        headers.add(new StreamHeader(key, value));
        return this;
    }

    @Override
    public Headers addUtf8(final String key, final String value) {
        Objects.requireNonNull(key, "Unable to add a null header key.");
        if (headers == null) {
            headers = new LinkedList<>();
        }
        headers.add(new StreamHeader(key, Utils.utf8(value)));
        return this;
    }

    @Override
    public Header lastWithName(final String key) {
        Objects.requireNonNull(key, "Header key cannot be null");
        if (headers != null) {
            final ListIterator<Header> iter = headers.listIterator(headers.size());
            while (iter.hasPrevious()) {
                final Header header = iter.previous();
                if (key.equals(header.key())) {
                    return header;
                }
            }
        }
        return null;
    }

    @Override
    public boolean hasWithName(final String key) {
        Objects.requireNonNull(key, "Header key cannot be null");
        if (headers != null) {
            for (final Header header : headers) {
                if (key.equals(header.key())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Iterator<Header> allWithName(final String key) {
        return new FilterByKeyIterator(iterator(), key);
    }

    @Override
    public Iterator<Header> iterator() {
        return headers == null ? Collections.emptyIterator() :
            headers.iterator();
    }


    @Override
    public Headers remove(final String key) {
        Objects.requireNonNull(key, "Header key cannot be null");
        if (!isEmpty()) {
            final Iterator<Header> iterator = iterator();
            while (iterator.hasNext()) {
                if (iterator.next().key().equals(key)) {
                    iterator.remove();
                }
            }
        }
        return this;
    }

    @Override
    public Headers retainLatest() {
        if (!isEmpty()) {
            final Set<String> keys = new HashSet<>();
            final ListIterator<Header> iter = headers.listIterator(headers.size());
            while (iter.hasPrevious()) {
                final Header header = iter.previous();
                final String key = header.key();
                if (!keys.add(key)) {
                    iter.remove();
                }
            }
        }
        return this;
    }

    @Override
    public Headers retainLatest(final String key) {
        Objects.requireNonNull(key, "Header key cannot be null");
        if (!isEmpty()) {
            boolean found = false;
            final ListIterator<Header> iter = headers.listIterator(headers.size());
            while (iter.hasPrevious()) {
                final String headerKey = iter.previous().key();
                if (key.equals(headerKey)) {
                    if (found) {
                        iter.remove();
                    }
                    found = true;
                }
            }
        }
        return this;
    }

    @Override
    public Headers apply(final String key, final Headers.HeaderTransform transform) {
        Objects.requireNonNull(key, "Header key cannot be null");
        if (!isEmpty()) {
            final ListIterator<Header> iter = headers.listIterator();
            while (iter.hasNext()) {
                final Header orig = iter.next();
                if (orig.key().equals(key)) {
                    final Header updated = transform.apply(orig);
                    if (updated != null) {
                        iter.set(updated);
                    } else {
                        iter.remove();
                    }
                }
            }
        }
        return this;
    }

    @Override
    public Headers apply(final Headers.HeaderTransform transform) {
        if (!isEmpty()) {
            final ListIterator<Header> iter = headers.listIterator();
            while (iter.hasNext()) {
                final Header orig = iter.next();
                final Header updated = transform.apply(orig);
                if (updated != null) {
                    iter.set(updated);
                } else {
                    iter.remove();
                }
            }
        }
        return this;
    }

    @Override
    public int hashCode() {
        return isEmpty() ? EMPTY_HASH : Objects.hash(headers);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof Headers) {
            final Headers that = (Headers) obj;
            final Iterator<Header> thisIter = this.iterator();
            final Iterator<Header> thatIter = that.iterator();
            while (thisIter.hasNext() && thatIter.hasNext()) {
                if (!Objects.equals(thisIter.next(), thatIter.next())) {
                    return false;
                }
            }
            return !thisIter.hasNext() && !thatIter.hasNext();
        }
        return false;
    }

    @Override
    public String toString() {
        return "StreamHeaders(headers=" + (headers != null ? headers : "") + ")";
    }

    @Override
    public StreamHeaders duplicate() {
        return new StreamHeaders(this);
    }


    @Override
    public org.apache.kafka.common.header.Headers unwrap() {
        final RecordHeaders headers = new RecordHeaders();
        if (!isEmpty()) {
            this.headers.forEach(header -> headers.add(header.key(), header.value()));
        }
        return headers;
    }

    private static final class FilterByKeyIterator extends
        AbstractIterator<Header> {

        private final Iterator<Header> original;
        private final String key;

        private FilterByKeyIterator(final Iterator<Header> original, final String key) {
            this.original = original;
            this.key = key;
        }

        @Override
        protected Header makeNext() {
            while (original.hasNext()) {
                final Header header = original.next();
                if (!header.key().equals(key)) {
                    continue;
                }
                return header;
            }
            return this.allDone();
        }
    }
}
