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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.PrimitiveRef;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * The transaction index maintains metadata about the aborted transactions for each segment. This includes
 * the start and end offsets for the aborted transactions and the last stable offset (LSO) at the time of
 * the abort. This index is used to find the aborted transactions in the range of a given fetch request at
 * the READ_COMMITTED isolation level.
 *
 * There is at most one transaction index for each log segment. The entries correspond to the transactions
 * whose commit markers were written in the corresponding log segment. Note, however, that individual transactions
 * may span multiple segments. Recovering the index therefore requires scanning the earlier segments in
 * order to find the start of the transactions.
 */
public class TransactionIndex implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(TransactionIndex.class);

    private static class AbortedTxnWithPosition {
        final AbortedTxn txn;
        final int position;
        AbortedTxnWithPosition(AbortedTxn txn, int position) {
            this.txn = txn;
            this.position = position;
        }
    }

    private final long startOffset;

    final TransactionIndexFile txnFile;

    private Long lastOffset = null;

    public TransactionIndex(long startOffset, File file) throws IOException {
        this.startOffset = startOffset;
        this.txnFile = new TransactionIndexFile(file.toPath());
    }

    public Path path() {
        return txnFile.path();
    }

    public File file() {
        return txnFile.path().toFile();
    }

    public void updateParentDir(File parentDir) {
        txnFile.updateParentDir(parentDir.toPath());
    }

    public void renameTo(File f) throws IOException {
        txnFile.renameTo(f.toPath());
    }

    public void flush() throws IOException {
        txnFile.flush();
    }

    /**
     * Remove all the entries from the index. Unlike `AbstractIndex`, this index is not resized ahead of time.
     */
    public void reset() throws IOException {
        txnFile.truncate(0);
        lastOffset = null;
    }

    public void close() throws IOException {
        txnFile.closeChannel();
    }

    /**
     * Delete this index.
     *
     * @throws IOException if deletion fails due to an I/O error
     * @return `true` if the file was deleted by this method; `false` if the file could not be deleted because it did
     *         not exist
     */
    public boolean deleteIfExists() throws IOException {
        return txnFile.deleteIfExists();
    }

    public void append(AbortedTxn abortedTxn) throws IOException {
        if (lastOffset != null) {
            if (lastOffset >= abortedTxn.lastOffset())
                throw new IllegalArgumentException("The last offset of appended transactions must increase sequentially, but "
                    + abortedTxn.lastOffset() + " is not greater than current last offset " + lastOffset + " of index "
                    + txnFile.path().toAbsolutePath());
        }
        lastOffset = abortedTxn.lastOffset();
        txnFile.write(abortedTxn.buffer.duplicate());
    }

    public void truncateTo(long offset) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(AbortedTxn.TOTAL_SIZE);
        Long newLastOffset = null;
        for (AbortedTxnWithPosition txnWithPosition : iterable(() -> buffer)) {
            AbortedTxn abortedTxn = txnWithPosition.txn;
            long position = txnWithPosition.position;
            if (abortedTxn.lastOffset() >= offset) {
                txnFile.truncate(position);
                lastOffset = newLastOffset;
                return;
            }
            newLastOffset = abortedTxn.lastOffset();
        }
    }

    public List<AbortedTxn> allAbortedTxns() {
        List<AbortedTxn> result = new ArrayList<>();
        for (AbortedTxnWithPosition txnWithPosition : iterable())
            result.add(txnWithPosition.txn);
        return result;
    }

    /**
     * Collect all aborted transactions which overlap with a given fetch range.
     *
     * @param fetchOffset Inclusive first offset of the fetch range
     * @param upperBoundOffset Exclusive last offset in the fetch range
     * @return An object containing the aborted transactions and whether the search needs to continue
     *         into the next log segment.
     */
    public TxnIndexSearchResult collectAbortedTxns(long fetchOffset, long upperBoundOffset) {
        List<AbortedTxn> abortedTransactions = new ArrayList<>();
        for (AbortedTxnWithPosition txnWithPosition : iterable()) {
            AbortedTxn abortedTxn = txnWithPosition.txn;
            if (abortedTxn.lastOffset() >= fetchOffset && abortedTxn.firstOffset() < upperBoundOffset)
                abortedTransactions.add(abortedTxn);

            if (abortedTxn.lastStableOffset() >= upperBoundOffset)
                return new TxnIndexSearchResult(abortedTransactions, true);
        }
        return new TxnIndexSearchResult(abortedTransactions, false);
    }

    /**
     * Do a basic sanity check on this index to detect obvious problems.
     *
     * @throws CorruptIndexException if any problems are found.
     */
    public void sanityCheck() {
        ByteBuffer buffer = ByteBuffer.allocate(AbortedTxn.TOTAL_SIZE);
        for (AbortedTxnWithPosition txnWithPosition : iterable(() -> buffer)) {
            AbortedTxn abortedTxn = txnWithPosition.txn;
            if (abortedTxn.lastOffset() < startOffset)
                throw new CorruptIndexException("Last offset of aborted transaction " + abortedTxn + " in index "
                    + txnFile.path().toAbsolutePath() + " is less than start offset " + startOffset);
        }
    }

    private Iterable<AbortedTxnWithPosition> iterable() {
        return iterable(() -> ByteBuffer.allocate(AbortedTxn.TOTAL_SIZE));
    }

    private Iterable<AbortedTxnWithPosition> iterable(Supplier<ByteBuffer> allocate) {
        if (!txnFile.exists())
            return Collections.emptyList();

        PrimitiveRef.IntRef position = PrimitiveRef.ofInt(0);

        return () -> new Iterator<AbortedTxnWithPosition>() {

            @Override
            public boolean hasNext() {
                try {
                    return txnFile.currentPosition() - position.value >= AbortedTxn.TOTAL_SIZE;
                } catch (IOException e) {
                    throw new KafkaException("Failed read position from the transaction index " + txnFile.path().toAbsolutePath(), e);
                }
            }

            @Override
            public AbortedTxnWithPosition next() {
                try {
                    ByteBuffer buffer = allocate.get();
                    txnFile.read(buffer, position.value);
                    buffer.flip();

                    AbortedTxn abortedTxn = new AbortedTxn(buffer);
                    if (abortedTxn.version() > AbortedTxn.CURRENT_VERSION)
                        throw new KafkaException("Unexpected aborted transaction version " + abortedTxn.version()
                            + " in transaction index " + txnFile.path().toAbsolutePath() + ", current version is "
                            + AbortedTxn.CURRENT_VERSION);
                    AbortedTxnWithPosition nextEntry = new AbortedTxnWithPosition(abortedTxn, position.value);
                    position.value += AbortedTxn.TOTAL_SIZE;
                    return nextEntry;
                } catch (IOException e) {
                    // We received an unexpected error reading from the index file. We propagate this as an
                    // UNKNOWN error to the consumer, which will cause it to retry the fetch.
                    throw new KafkaException("Failed to read from the transaction index " + txnFile.path().toAbsolutePath(), e);
                }
            }

        };
    }

    // Visible for testing
    static class TransactionIndexFile {
        // note that the file is not created until we need it
        private volatile Path path;
        // channel is reopened as long as there are reads and writes
        private FileChannel channel;

        TransactionIndexFile(Path path) throws IOException {
            this.path = path;

            if (Files.exists(path))
                openChannel();
        }

        private void openChannel() throws IOException {
            channel = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
            );
            channel.position(channel.size());
        }

        synchronized void updateParentDir(Path parentDir) {
            this.path = parentDir.resolve(path.getFileName());
        }

        synchronized void renameTo(Path other) throws IOException {
            try {
                if (Files.exists(path))
                    Utils.atomicMoveWithFallback(path, other, false);
            } finally {
                this.path = other;
            }
        }

        void flush() throws IOException {
            if (channel != null)
                channel.force(true);
        }

        void closeChannel() throws IOException {
            if (channel != null)
                channel.close();
        }

        Path path() {
            return path;
        }

        void truncate(long position) throws IOException {
            if (channel != null)
                channel.truncate(position);
        }

        boolean exists() {
            return Files.exists(path);
        }

        boolean deleteIfExists() throws IOException {
            closeChannel();
            return Files.deleteIfExists(path());
        }

        void write(ByteBuffer buffer) throws IOException {
            Utils.writeFully(channel(), buffer);
        }

        void read(ByteBuffer buffer, int position) throws IOException {
            Utils.readFully(channel(), buffer, position);
        }

        long currentPosition() throws IOException {
            return channel().position();
        }

        /**
         * Use to read or write values to the index.
         * The file is the source of truth and if available values should be read from or written to.
         *
         * @return an open file channel with the position at the end of the file
         * @throws IOException if any I/O error happens, but not if existing channel is closed.
         *                     In that case, it is reopened.
         */
        private FileChannel channel() throws IOException {
            if (channel == null) {
                openChannel();
            } else {
                // as channel is exposed, it could be closed without setting it to null
                if (!channel.isOpen())  {
                    log.debug("Transaction index channel was closed directly and is going to be reopened");
                    openChannel();
                }
            }
            return channel;
        }
    }
}
