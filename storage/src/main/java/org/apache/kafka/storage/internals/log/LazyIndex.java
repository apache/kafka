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
package org.apache.kafka.storage.internals.log;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.common.utils.Utils;

/**
 * A wrapper over an `AbstractIndex` instance that provides a mechanism to defer loading
 * (i.e. memory mapping) the underlying index until it is accessed for the first time via the
 * `get` method.
 *
 * In addition, this class exposes a number of methods (e.g. updateParentDir, renameTo, close,
 * etc.) that provide the desired behavior without causing the index to be loaded. If the index
 * had previously been loaded, the methods in this class simply delegate to the relevant method in
 * the index.
 *
 * This is an important optimization with regards to broker start-up and shutdown time if it has a
 * large number of segments.
 *
 * Methods of this class are thread safe. Make sure to check `AbstractIndex` subclasses
 * documentation to establish their thread safety.
 */
public class LazyIndex<T extends AbstractIndex> implements Closeable {

    private enum IndexType {
      OFFSET, TIME
    }

    private interface IndexWrapper extends Closeable {
        File file();
        void updateParentDir(File file);
        void renameTo(File file) throws IOException;
        boolean deleteIfExists() throws IOException;
        void close() throws IOException;
        void closeHandler();
    }

    private static class IndexFile implements IndexWrapper {

        private volatile File file;

        IndexFile(File file) {
            this.file = file;
        }

        @Override
        public File file() {
            return file;
        }

        @Override
        public void updateParentDir(File parentDir) {
            file = new File(parentDir, file.getName());
        }

        @Override
        public void renameTo(File f) throws IOException {
            try {
                Utils.atomicMoveWithFallback(file.toPath(), f.toPath(), false);
            } catch (NoSuchFileException e) {
                if (file.exists())
                    throw e;
            } finally {
                file = f;
            }
        }

        @Override
        public boolean deleteIfExists() throws IOException {
            return Files.deleteIfExists(file.toPath());
        }

        @Override
        public void close() { }

        @Override
        public void closeHandler() { }

    }

    private static class IndexValue<T extends AbstractIndex> implements IndexWrapper {

        private final T index;

        IndexValue(T index) {
            this.index = index;
        }

        @Override
        public File file() {
            return index.file();
        }

        @Override
        public void updateParentDir(File parentDir) {
            index.updateParentDir(parentDir);
        }

        @Override
        public void renameTo(File f) throws IOException {
            index.renameTo(f);
        }

        @Override
        public boolean deleteIfExists() throws IOException {
            return index.deleteIfExists();
        }

        @Override
        public void close() throws IOException {
            index.close();
        }

        @Override
        public void closeHandler() {
            index.closeHandler();
        }
    }

    private final Lock lock = new ReentrantLock();
    private final long baseOffset;
    private final int maxIndexSize;
    private final IndexType indexType;

    private volatile IndexWrapper indexWrapper;

    private LazyIndex(IndexWrapper indexWrapper, long baseOffset, int maxIndexSize, IndexType indexType) {
        this.indexWrapper = indexWrapper;
        this.baseOffset = baseOffset;
        this.maxIndexSize = maxIndexSize;
        this.indexType = indexType;
    }

    public static LazyIndex<OffsetIndex> forOffset(File file, long baseOffset, int maxIndexSize) {
        return new LazyIndex<>(new IndexFile(file), baseOffset, maxIndexSize, IndexType.OFFSET);
    }

    public static LazyIndex<TimeIndex> forTime(File file, long baseOffset, int maxIndexSize) {
        return new LazyIndex<>(new IndexFile(file), baseOffset, maxIndexSize, IndexType.TIME);
    }

    public File file() {
        return indexWrapper.file();
    }

    @SuppressWarnings("unchecked")
    public T get() throws IOException {
        IndexWrapper wrapper = indexWrapper;
        if (wrapper instanceof IndexValue<?>)
            return ((IndexValue<T>) wrapper).index;
        else {
            lock.lock();
            try {
                if (indexWrapper instanceof IndexValue<?>)
                    return ((IndexValue<T>) indexWrapper).index;
                else if (indexWrapper instanceof IndexFile) {
                    IndexFile indexFile = (IndexFile) indexWrapper;
                    IndexValue<T> indexValue = new IndexValue<>(loadIndex(indexFile.file));
                    indexWrapper = indexValue;
                    return indexValue.index;
                } else
                    throw new IllegalStateException("Unexpected type for indexWrapper " + indexWrapper.getClass());
            } finally {
                lock.unlock();
            }
        }
    }

    public void updateParentDir(File parentDir) {
        lock.lock();
        try {
            indexWrapper.updateParentDir(parentDir);
        } finally {
            lock.unlock();
        }
    }

    public void renameTo(File f) throws IOException {
        lock.lock();
        try {
            indexWrapper.renameTo(f);
        } finally {
            lock.unlock();
        }
    }

    public boolean deleteIfExists() throws IOException {
        lock.lock();
        try {
            return indexWrapper.deleteIfExists();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            indexWrapper.close();
        } finally {
            lock.unlock();
        }
    }

    public void closeHandler() {
        lock.lock();
        try {
            indexWrapper.closeHandler();
        } finally {
            lock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    private T loadIndex(File file) throws IOException {
        switch (indexType) {
            case OFFSET:
                return (T) new OffsetIndex(file, baseOffset, maxIndexSize, true);
            case TIME:
                return (T) new TimeIndex(file, baseOffset, maxIndexSize, true);
            default:
                throw new IllegalStateException("Unexpected indexType " + indexType);
        }
    }

}
