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

    public StreamHeaders(Iterable<Header> original) {
        if (original == null) {
            return;
        }
        if (original instanceof StreamHeaders) {
            StreamHeaders originalHeaders = (StreamHeaders) original;
            if (!originalHeaders.isEmpty()) {
                headers = new LinkedList<>(originalHeaders.headers);
            }
        } else {
            headers = new LinkedList<>();
            for (Header header : original) {
                Objects.requireNonNull(header, "Unable to add a null header.");
                headers.add(header);
            }
        }
    }


    public static Headers wrap(org.apache.kafka.common.header.Headers originals) {
        LinkedList<Header> headers = new LinkedList<>();
        originals.forEach(header -> headers.add(StreamHeader.wrap(header)));
        return new StreamHeaders(headers);
    }

    public static Headers wrap(org.apache.kafka.common.header.Header[] originals) {
        LinkedList<Header> headers = new LinkedList<>();
        for (org.apache.kafka.common.header.Header header : originals) {
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
    public Headers add(Header header) {
        Objects.requireNonNull(header, "Unable to add a null header.");
        if (headers == null) {
            headers = new LinkedList<>();
        }
        headers.add(header);
        return this;
    }

    @Override
    public Headers add(String key, byte[] value) {
        Objects.requireNonNull(key, "Unable to add a null header key.");
        if (headers == null) {
            headers = new LinkedList<>();
        }
        headers.add(new StreamHeader(key, value));
        return this;
    }

    @Override
    public Headers addUtf8(String key, String value) {
        Objects.requireNonNull(key, "Unable to add a null header key.");
        if (headers == null) {
            headers = new LinkedList<>();
        }
        headers.add(new StreamHeader(key, Utils.utf8(value)));
        return this;
    }

    @Override
    public Header lastWithName(String key) {
        Objects.requireNonNull(key, "Header key cannot be null");
        if (headers != null) {
            ListIterator<Header> iter = headers.listIterator(headers.size());
            while (iter.hasPrevious()) {
                Header header = iter.previous();
                if (key.equals(header.key())) {
                    return header;
                }
            }
        }
        return null;
    }

    @Override
    public boolean hasWithName(String key) {
        Objects.requireNonNull(key, "Header key cannot be null");
        if (headers != null) {
            for (Header header : headers) {
                if (key.equals(header.key())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Iterator<Header> allWithName(String key) {
        return new FilterByKeyIterator(iterator(), key);
    }

    @Override
    public Iterator<Header> iterator() {
        return headers == null ? Collections.emptyIterator() :
            headers.iterator();
    }


    @Override
    public Headers remove(String key) {
        Objects.requireNonNull(key, "Header key cannot be null");
        if (!isEmpty()) {
            Iterator<Header> iterator = iterator();
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
            Set<String> keys = new HashSet<>();
            ListIterator<Header> iter = headers.listIterator(headers.size());
            while (iter.hasPrevious()) {
                Header header = iter.previous();
                String key = header.key();
                if (!keys.add(key)) {
                    iter.remove();
                }
            }
        }
        return this;
    }

    @Override
    public Headers retainLatest(String key) {
        Objects.requireNonNull(key, "Header key cannot be null");
        if (!isEmpty()) {
            boolean found = false;
            ListIterator<Header> iter = headers.listIterator(headers.size());
            while (iter.hasPrevious()) {
                String headerKey = iter.previous().key();
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
    public Headers apply(String key, Headers.HeaderTransform transform) {
        Objects.requireNonNull(key, "Header key cannot be null");
        if (!isEmpty()) {
            ListIterator<Header> iter = headers.listIterator();
            while (iter.hasNext()) {
                Header orig = iter.next();
                if (orig.key().equals(key)) {
                    Header updated = transform.apply(orig);
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
    public Headers apply(Headers.HeaderTransform transform) {
        if (!isEmpty()) {
            ListIterator<Header> iter = headers.listIterator();
            while (iter.hasNext()) {
                Header orig = iter.next();
                Header updated = transform.apply(orig);
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
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof Headers) {
            Headers that = (Headers) obj;
            Iterator<Header> thisIter = this.iterator();
            Iterator<Header> thatIter = that.iterator();
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
        return "ConnectHeaders(headers=" + (headers != null ? headers : "") + ")";
    }

    @Override
    public StreamHeaders duplicate() {
        return new StreamHeaders(this);
    }


    @Override
    public org.apache.kafka.common.header.Headers unwrap() {
        RecordHeaders headers = new RecordHeaders();
        if (!isEmpty()) {
            this.headers.forEach(header -> headers.add(header.key(), header.value()));
        }
        return headers;
    }

    private static final class FilterByKeyIterator extends
        AbstractIterator<Header> {

        private final Iterator<Header> original;
        private final String key;

        private FilterByKeyIterator(Iterator<Header> original, String key) {
            this.original = original;
            this.key = key;
        }

        @Override
        protected Header makeNext() {
            while (original.hasNext()) {
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
