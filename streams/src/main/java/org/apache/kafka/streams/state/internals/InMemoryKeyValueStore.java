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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryKeyValueStore implements KeyValueStore<Bytes, byte[]> {
    private static final Logger log = LoggerFactory.getLogger(InMemoryKeyValueStore.class);

    private static final String IN_MEMORY_KEY_VALUE_STORE_THREAD_PREFIX = "kafka-in-memory-key-value-store-thread-";

    public static final String INMEM_EXTENSION = ".inmem";

    public static final int COUNT_FLUSH_TO_STORE = 10;

    private final String name;
    private TreeMap<Bytes, byte[]> map = new TreeMap<>();
    private volatile boolean open = false;
    private long size = 0L; // SkipListMap#size is O(N) so we just do our best to track it

    private final InMemoryKeyValueStorePersistThread persistThread;
    private Path storeDir;
    private AtomicInteger flushCounter;

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryKeyValueStore.class);

    public InMemoryKeyValueStore(final String name, final boolean persistent) {
        this.name = name;
        this.persistThread = persistent ? new InMemoryKeyValueStorePersistThread(name) : null;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {

        if (persistThread != null) {
            flushCounter = new AtomicInteger();

            storeDir = Paths.get(context.stateDir().getAbsolutePath(), name);

            if (!Files.exists(storeDir)) {
                try {
                    Files.createDirectories(storeDir);
                } catch (final IOException fatal) {
                    throw new ProcessorStateException(fatal);
                }
            } else {
                loadStore();
            }

            persistThread.start();
        }

        if (root != null) {
            // register the store
            context.register(root, (key, value) -> put(Bytes.wrap(key), value));
        }

        open = true;
    }

    @Override
    public boolean persistent() {
        return persistThread != null;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public synchronized byte[] get(final Bytes key) {
        return map.get(key);
    }

    @Override
    public synchronized void put(final Bytes key, final byte[] value) {
        if (value == null) {
            size -= map.remove(key) == null ? 0 : 1;
        } else {
            size += map.put(key, value) == null ? 1 : 0;
        }
    }

    @Override
    public synchronized byte[] putIfAbsent(final Bytes key, final byte[] value) {
        final byte[] originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        for (final KeyValue<Bytes, byte[]> entry : entries) {
            put(entry.key, entry.value);
        }
    }

    @Override
    public synchronized byte[] delete(final Bytes key) {
        final byte[] oldValue = map.remove(key);
        size -= oldValue == null ? 0 : 1;
        return oldValue;
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {

        if (from.compareTo(to) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. "
                + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        return new DelegatingPeekingKeyValueIterator<>(
            name,
            new InMemoryKeyValueIterator(map.subMap(from, true, to, true).keySet()));
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> all() {
        return new DelegatingPeekingKeyValueIterator<>(
            name,
            new InMemoryKeyValueIterator(map.keySet()));
    }

    @Override
    public long approximateNumEntries() {
        return size;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void flush() {
        if (persistThread == null)
            return;

        final int flushCount = flushCounter.incrementAndGet();
        if (flushCount % COUNT_FLUSH_TO_STORE != 0)
            return;

        synchronized (this) {
            persistThread.mapToStore = (TreeMap<Bytes, byte[]>) map.clone();

            synchronized (persistThread) {
                persistThread.notify();
            }
        }
    }

    @Override
    public void close() {
        map.clear();
        size = 0;
        open = false;
        if (persistThread != null) {
            synchronized (persistThread) {
                persistThread.notify();
            }
        }
    }

    private class InMemoryKeyValueIterator implements KeyValueIterator<Bytes, byte[]> {
        private final Iterator<Bytes> iter;

        private InMemoryKeyValueIterator(final Set<Bytes> keySet) {
            this.iter = new TreeSet<>(keySet).iterator();
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            final Bytes key = iter.next();
            return new KeyValue<>(key, map.get(key));
        }

        @Override
        public void close() {
            // do nothing
        }

        @Override
        public Bytes peekNextKey() {
            throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
        }
    }

    @SuppressWarnings("unchecked")
    private void loadStore() {
        try {
            final List<Path> files = Files.list(storeDir)
                .filter(p -> p.toString().endsWith(INMEM_EXTENSION))
                .sorted((p1, p2) -> (int) (fileNameTimestamp(p2) - fileNameTimestamp(p1)))
                .collect(Collectors.toList());

            for (final Path file : files) {
                try (ObjectInputStream oos = new ObjectInputStream(Files.newInputStream(file))) {
                    this.map = (TreeMap<Bytes, byte[]>) oos.readObject();
                    this.size = map.size();
                    this.persistThread.lastWritten = fileNameTimestamp(file);
                    if (log.isInfoEnabled())
                        log.info("InMemoryKeyValueStore loaded from file {}", file);
                    return;
                } catch (final IOException | ClassNotFoundException e) {
                    if (log.isWarnEnabled())
                        log.warn("InMemoryKeyValueStore corrupted file skipped {}", file);
                }
            }
        } catch (final IOException e) {
            if (log.isWarnEnabled())
                log.warn("Error listing file", e);
        }
    }

    private long fileNameTimestamp(final Path file) {
        final String fileName = file.toFile().getName();
        return Long.parseLong(fileName.substring(0, fileName.length() - INMEM_EXTENSION.length()));
    }

    private class InMemoryKeyValueStorePersistThread extends KafkaThread {
        volatile Map<Bytes, byte[]> mapToStore;

        long lastWritten;

        InMemoryKeyValueStorePersistThread(final String name) {
            super(IN_MEMORY_KEY_VALUE_STORE_THREAD_PREFIX + name, false);
        }

        @Override
        public void run() {
            Map<Bytes, byte[]> localMap = null;

            while (open) {
                // If already store current iteration then must wait for a next one.
                if (localMap == mapToStore) {
                    synchronized (this) {
                        // Check if iteration changed or closed after first check.
                        if (localMap == mapToStore && open) {
                            try {
                                // Waiting for a next iteration.
                                this.wait();
                            } catch (final InterruptedException e) {
                                // ignore.
                            }
                        }

                        if (!open)
                            return;
                    }
                }

                localMap = mapToStore;
                lastWritten = writeStoreToDisk(localMap, lastWritten);
            }
        }

        private long writeStoreToDisk(final Map<Bytes, byte[]> map, final long lastWritten) {
            final long now = System.currentTimeMillis();

            try (ObjectOutputStream oos =
                     new ObjectOutputStream(Files.newOutputStream(storeDir.resolve(now + INMEM_EXTENSION)))) {
                oos.writeObject(map);
            } catch (final IOException e) {
                throw new KafkaStorageException(e);
            }

            if (log.isDebugEnabled())
                log.debug("InMemoryKeyValueStore[{}] saved to file {}", name, now + INMEM_EXTENSION);

            if (now != lastWritten && lastWritten != 0) {
                try {
                    Files.delete(storeDir.resolve(lastWritten + INMEM_EXTENSION));
                } catch (final IOException e) {
                    if (log.isWarnEnabled()) {
                        log.warn("Can't remove InMemoryKeyValueStore[{}] file:", name, e);
                    }
                }
            }

            return now;
        }
    }
}
