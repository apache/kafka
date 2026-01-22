/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.state.HeadersBytesStore;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatchInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

import static org.apache.kafka.streams.state.HeadersBytesStore.convertToHeaderFormat;

/**
 * A persistent key-(value-timestamp-headers) store based on RocksDB.
 *
 * This is analogous to {@link RocksDBTimestampedStore}, but the "new" column family stores
 * a header-aware format. Legacy values (without headers) are converted on the fly using
 * {@link HeadersBytesStore#convertToHeaderFormat(byte[], byte[])}.
 */
public class RocksDBTimestampedStoreWithHeaders extends RocksDBStore implements HeadersBytesStore {

  private static final Logger log = LoggerFactory.getLogger(RocksDBTimestampedStoreWithHeaders.class);

  // New column family for header-aware timestamped values.
  // You can rename this if you prefer a different CF name.
  private static final byte[] TIMESTAMPED_VALUES_WITH_HEADERS_CF_NAME =
      "keyValueWithTimestampAndHeaders".getBytes(StandardCharsets.UTF_8);

  public RocksDBTimestampedStoreWithHeaders(final String name,
                                            final String metricsScope) {
    super(name, metricsScope);
  }

  RocksDBTimestampedStoreWithHeaders(final String name,
                                     final String parentDir,
                                     final RocksDBMetricsRecorder metricsRecorder) {
    super(name, parentDir, metricsRecorder);
  }

  @Override
  void openRocksDB(final DBOptions dbOptions,
                   final ColumnFamilyOptions columnFamilyOptions) {
    // We open two CFs:
    //  - DEFAULT_COLUMN_FAMILY: legacy (non-header) timestamped values
    //  - TIMESTAMPED_VALUES_WITH_HEADERS_CF_NAME: new header-aware format
    //
    // On first open with no legacy data, we just use the new CF.
    final List<ColumnFamilyHandle> columnFamilies = openRocksDB(
        dbOptions,
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
        new ColumnFamilyDescriptor(TIMESTAMPED_VALUES_WITH_HEADERS_CF_NAME, columnFamilyOptions)
    );

    final ColumnFamilyHandle legacyCf = columnFamilies.get(0);
    final ColumnFamilyHandle headersCf = columnFamilies.get(1);

    final RocksIterator legacyIter = db.newIterator(legacyCf);
    legacyIter.seekToFirst();
    if (legacyIter.isValid()) {
      log.info("Opening store {} in upgrade mode (legacy timestamped -> header-aware timestamped)", name);
      cfAccessor = new DualColumnFamilyAccessor(legacyCf, headersCf);
    } else {
      log.info("Opening store {} in regular header-aware mode", name);
      cfAccessor = new SingleColumnFamilyAccessor(headersCf);
      legacyCf.close();
    }
    legacyIter.close();
  }

  /**
   * Accessor that supports dual-column-family upgrade: legacy CF (no headers)
   * and new CF (header-aware format).
   */
  private class DualColumnFamilyAccessor implements ColumnFamilyAccessor {

    private final ColumnFamilyHandle oldColumnFamily;
    private final ColumnFamilyHandle newColumnFamily;

    private DualColumnFamilyAccessor(final ColumnFamilyHandle oldColumnFamily,
                                     final ColumnFamilyHandle newColumnFamily) {
      this.oldColumnFamily = oldColumnFamily;
      this.newColumnFamily = newColumnFamily;
    }

    @Override
    public void put(final DBAccessor accessor,
                    final byte[] key,
                    final byte[] valueWithHeaders) {
      synchronized (position) {
        if (valueWithHeaders == null) {
          try {
            accessor.delete(oldColumnFamily, key);
          } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error while removing key from store " + name, e);
          }
          try {
            accessor.delete(newColumnFamily, key);
          } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error while removing key from store " + name, e);
          }
        } else {
          try {
            accessor.delete(oldColumnFamily, key);
          } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error while removing key from store " + name, e);
          }
          try {
            accessor.put(newColumnFamily, key, valueWithHeaders);
            StoreQueryUtils.updatePosition(position, context);
          } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error while putting key/value into store " + name, e);
          }
        }
      }
    }

    @Override
    public void prepareBatch(final List<KeyValue<Bytes, byte[]>> entries,
                             final WriteBatchInterface batch) throws RocksDBException {
      for (final KeyValue<Bytes, byte[]> entry : entries) {
        Objects.requireNonNull(entry.key, "key cannot be null");
        addToBatch(entry.key.get(), entry.value, batch);
      }
    }

    @Override
    public byte[] get(final DBAccessor accessor, final byte[] key) throws RocksDBException {
      return get(accessor, key, Optional.empty());
    }

    @Override
    public byte[] get(final DBAccessor accessor,
                      final byte[] key,
                      final ReadOptions readOptions) throws RocksDBException {
      return get(accessor, key, Optional.of(readOptions));
    }

    private byte[] get(final DBAccessor accessor,
                       final byte[] key,
                       final Optional<ReadOptions> readOptions) throws RocksDBException {
      final byte[] valueWithHeaders =
          readOptions.isPresent()
              ? accessor.get(newColumnFamily, readOptions.get(), key)
              : accessor.get(newColumnFamily, key);

      if (valueWithHeaders != null) {
        return valueWithHeaders;
      }

      final byte[] legacyValue =
          readOptions.isPresent()
              ? accessor.get(oldColumnFamily, readOptions.get(), key)
              : accessor.get(oldColumnFamily, key);

      if (legacyValue != null) {
        // Convert legacy timestamped value into new header-aware format.
        final byte[] converted = convertToHeaderFormat(key, legacyValue);
        // We can eagerly write back the converted value to new CF.
        put(accessor, key, converted);
        return converted;
      }

      return null;
    }

    @Override
    public byte[] getOnly(final DBAccessor accessor,
                          final byte[] key) throws RocksDBException {
      final byte[] valueWithHeaders = accessor.get(newColumnFamily, key);
      if (valueWithHeaders != null) {
        return valueWithHeaders;
      }

      final byte[] legacyValue = accessor.get(oldColumnFamily, key);
      if (legacyValue != null) {
        // For "getOnly", we must NOT mutate state; just convert on the fly.
        return convertToHeaderFormat(key, legacyValue);
      }
      return null;
    }

    @Override
    public ManagedKeyValueIterator<Bytes, byte[]> range(final DBAccessor accessor,
                                                        final Bytes from,
                                                        final Bytes to,
                                                        final boolean forward) {
      return new RocksDBDualCFRangeIterator(
          name,
          accessor.newIterator(newColumnFamily),
          accessor.newIterator(oldColumnFamily),
          from,
          to,
          forward,
          /* toInclusive = */ true
      );
    }

    @Override
    public void deleteRange(final DBAccessor accessor,
                            final byte[] from,
                            final byte[] to) {
      try {
        accessor.deleteRange(oldColumnFamily, from, to);
      } catch (final RocksDBException e) {
        throw new ProcessorStateException("Error while removing key from store " + name, e);
      }
      try {
        accessor.deleteRange(newColumnFamily, from, to);
      } catch (final RocksDBException e) {
        throw new ProcessorStateException("Error while removing key from store " + name, e);
      }
    }

    @Override
    public ManagedKeyValueIterator<Bytes, byte[]> all(final DBAccessor accessor,
                                                      final boolean forward) {
      final RocksIterator iterWithHeaders = accessor.newIterator(newColumnFamily);
      final RocksIterator iterLegacy = accessor.newIterator(oldColumnFamily);
      if (forward) {
        iterWithHeaders.seekToFirst();
        iterLegacy.seekToFirst();
      } else {
        iterWithHeaders.seekToLast();
        iterLegacy.seekToLast();
      }
      return new RocksDBDualCFIterator(name, iterWithHeaders, iterLegacy, forward);
    }

    @Override
    public ManagedKeyValueIterator<Bytes, byte[]> prefixScan(final DBAccessor accessor,
                                                             final Bytes prefix) {
      final Bytes to = incrementWithoutOverflow(prefix);
      return new RocksDBDualCFRangeIterator(
          name,
          accessor.newIterator(newColumnFamily),
          accessor.newIterator(oldColumnFamily),
          prefix,
          to,
          true,
          /* toInclusive = */ false
      );
    }

    @Override
    public long approximateNumEntries(final DBAccessor accessor) throws RocksDBException {
      return accessor.approximateNumEntries(oldColumnFamily)
          + accessor.approximateNumEntries(newColumnFamily);
    }

    @Override
    public void flush(final DBAccessor accessor) throws RocksDBException {
      accessor.flush(oldColumnFamily, newColumnFamily);
    }

    @Override
    public void addToBatch(final byte[] key,
                           final byte[] value,
                           final WriteBatchInterface batch) throws RocksDBException {
      if (value == null) {
        batch.delete(oldColumnFamily, key);
        batch.delete(newColumnFamily, key);
      } else {
        batch.delete(oldColumnFamily, key);
        batch.put(newColumnFamily, key, value);
      }
    }

    @Override
    public void close() {
      oldColumnFamily.close();
      newColumnFamily.close();
    }
  }

  /**
   * Iterator that merges the new (header-aware) CF and legacy CF, converting legacy values
   * to the new format on the fly.
   */
  private static class RocksDBDualCFIterator
      extends AbstractIterator<KeyValue<Bytes, byte[]>>
      implements ManagedKeyValueIterator<Bytes, byte[]> {

    // RocksDB's JNI interface does not expose getters/setters that allow the
    // comparator to be pluggable, and the default is lexicographic, so it's
    // safe to just force lexicographic comparator here for now.
    private final Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;

    private final String storeName;
    private final RocksIterator iterWithHeaders;
    private final RocksIterator iterLegacy;
    private final boolean forward;

    private volatile boolean open = true;

    private byte[] nextWithHeaders;
    private byte[] nextLegacy;
    private KeyValue<Bytes, byte[]> next;
    private Runnable closeCallback = null;

    RocksDBDualCFIterator(final String storeName,
                          final RocksIterator iterWithHeaders,
                          final RocksIterator iterLegacy,
                          final boolean forward) {
      this.iterWithHeaders = iterWithHeaders;
      this.iterLegacy = iterLegacy;
      this.storeName = storeName;
      this.forward = forward;
    }

    @Override
    public synchronized boolean hasNext() {
      if (!open) {
        throw new InvalidStateStoreException(
            String.format("RocksDB iterator for store %s has closed", storeName)
        );
      }
      return super.hasNext();
    }

    @Override
    public synchronized KeyValue<Bytes, byte[]> next() {
      return super.next();
    }

    @Override
    protected KeyValue<Bytes, byte[]> makeNext() {
      if (nextLegacy == null && iterLegacy.isValid()) {
        nextLegacy = iterLegacy.key();
      }
      if (nextWithHeaders == null && iterWithHeaders.isValid()) {
        nextWithHeaders = iterWithHeaders.key();
      }

      if (nextLegacy == null && !iterLegacy.isValid()) {
        if (nextWithHeaders == null && !iterWithHeaders.isValid()) {
          return allDone();
        } else {
          next = KeyValue.pair(new Bytes(nextWithHeaders), iterWithHeaders.value());
          nextWithHeaders = null;
          if (forward) {
            iterWithHeaders.next();
          } else {
            iterWithHeaders.prev();
          }
        }
      } else {
        if (nextWithHeaders == null) {
          next = KeyValue.pair(
              new Bytes(nextLegacy),
              convertToHeaderFormat(iterLegacy.key(), iterLegacy.value())
          );
          nextLegacy = null;
          if (forward) {
            iterLegacy.next();
          } else {
            iterLegacy.prev();
          }
        } else {
          if (forward) {
            if (comparator.compare(nextLegacy, nextWithHeaders) <= 0) {
              next = KeyValue.pair(
                  new Bytes(nextLegacy),
                  convertToHeaderFormat(iterLegacy.key(), iterLegacy.value())
              );
              nextLegacy = null;
              iterLegacy.next();
            } else {
              next = KeyValue.pair(new Bytes(nextWithHeaders), iterWithHeaders.value());
              nextWithHeaders = null;
              iterWithHeaders.next();
            }
          } else {
            if (comparator.compare(nextLegacy, nextWithHeaders) >= 0) {
              next = KeyValue.pair(
                  new Bytes(nextLegacy),
                  convertToHeaderFormat(iterLegacy.key(), iterLegacy.value())
              );
              nextLegacy = null;
              iterLegacy.prev();
            } else {
              next = KeyValue.pair(new Bytes(nextWithHeaders), iterWithHeaders.value());
              nextWithHeaders = null;
              iterWithHeaders.prev();
            }
          }
        }
      }
      return next;
    }

    @Override
    public synchronized void close() {
      if (closeCallback == null) {
        throw new IllegalStateException(
            "RocksDBDualCFIterator expects close callback to be set immediately upon creation"
        );
      }
      closeCallback.run();
      iterLegacy.close();
      iterWithHeaders.close();
      open = false;
    }

    @Override
    public Bytes peekNextKey() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return next.key;
    }

    @Override
    public void onClose(final Runnable closeCallback) {
      this.closeCallback = closeCallback;
    }
  }

  private static class RocksDBDualCFRangeIterator extends RocksDBDualCFIterator {

    // same comparator semantics as parent
    private final Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;

    private final byte[] rawLastKey;
    private final boolean forward;
    private final boolean toInclusive;

    RocksDBDualCFRangeIterator(final String storeName,
                               final RocksIterator iterWithHeaders,
                               final RocksIterator iterLegacy,
                               final Bytes from,
                               final Bytes to,
                               final boolean forward,
                               final boolean toInclusive) {
      super(storeName, iterWithHeaders, iterLegacy, forward);
      this.forward = forward;
      this.toInclusive = toInclusive;

      if (forward) {
        if (from == null) {
          iterWithHeaders.seekToFirst();
          iterLegacy.seekToFirst();
        } else {
          iterWithHeaders.seek(from.get());
          iterLegacy.seek(from.get());
        }
        rawLastKey = to == null ? null : to.get();
      } else {
        if (to == null) {
          iterWithHeaders.seekToLast();
          iterLegacy.seekToLast();
        } else {
          iterWithHeaders.seekForPrev(to.get());
          iterLegacy.seekForPrev(to.get());
        }
        rawLastKey = from == null ? null : from.get();
      }
    }

    @Override
    protected KeyValue<Bytes, byte[]> makeNext() {
      final KeyValue<Bytes, byte[]> next = super.makeNext();
      if (next == null) {
        return allDone();
      } else if (rawLastKey == null) {
        // null means range endpoint is open
        return next;
      } else {
        if (forward) {
          if (comparator.compare(next.key.get(), rawLastKey) < 0) {
            return next;
          } else if (comparator.compare(next.key.get(), rawLastKey) == 0) {
            return toInclusive ? next : allDone();
          } else {
            return allDone();
          }
        } else {
          if (comparator.compare(next.key.get(), rawLastKey) >= 0) {
            return next;
          } else {
            return allDone();
          }
        }
      }
    }
  }
}