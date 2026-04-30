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
import org.apache.kafka.common.message.AbortedTxn;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.utils.Utils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

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

    // Note: if new fields are added to AbortedTxn, this code may need to be changed to read the
    // version bytes first for each record and then determine the record body size based on the version.
    private static final int ABORTED_TXN_RECORD_SIZE =
        MessageUtil.toVersionPrefixedByteBuffer(AbortedTxn.HIGHEST_SUPPORTED_VERSION, new AbortedTxn()).remaining();

    private record AbortedTxnWithPosition(AbortedTxn txn, int position) {
    }

    private final long startOffset;

    private volatile File file;

    // note that the file is not created until we need it
    private Optional<FileChannel> maybeChannel = Optional.empty();
    private OptionalLong lastOffset = OptionalLong.empty();

    public TransactionIndex(long startOffset, File file) throws IOException {
        this.startOffset = startOffset;
        this.file = file;

        if (file.exists())
            openChannel();
    }

    public File file() {
        return file;
    }

    public void updateParentDir(File parentDir) {
        this.file = new File(parentDir, file.getName());
    }

    public void append(AbortedTxn abortedTxn) throws IOException {
        lastOffset.ifPresent(offset -> {
            if (offset >= abortedTxn.lastOffset())
                throw new IllegalArgumentException("The last offset of appended transactions must increase sequentially, but "
                    + abortedTxn.lastOffset() + " is not greater than current last offset " + offset + " of index "
                    + file.getAbsolutePath());
        });
        lastOffset = OptionalLong.of(abortedTxn.lastOffset());
        ByteBuffer buffer = MessageUtil.toVersionPrefixedByteBuffer(AbortedTxn.HIGHEST_SUPPORTED_VERSION, abortedTxn);
        Utils.writeFully(channel(), buffer);
    }

    public void flush() throws IOException {
        FileChannel channel = channelOrNull();
        if (channel != null)
            channel.force(true);
    }

    /**
     * Remove all the entries from the index. Unlike `AbstractIndex`, this index is not resized ahead of time.
     */
    public void reset() throws IOException {
        FileChannel channel = channelOrNull();
        if (channel != null)
            channel.truncate(0);
        lastOffset = OptionalLong.empty();
    }

    public void close() throws IOException {
        FileChannel channel = channelOrNull();
        if (channel != null && channel.isOpen())
            channel.close();
        maybeChannel = Optional.empty();
    }

    /**
     * Delete this index.
     *
     * @throws IOException if deletion fails due to an I/O error
     * @return `true` if the file was deleted by this method; `false` if the file could not be deleted because it did
     *         not exist
     */
    public boolean deleteIfExists() throws IOException {
        close();
        return Files.deleteIfExists(file.toPath());
    }

    public void renameTo(File f) throws IOException {
        try {
            if (file.exists())
                Utils.atomicMoveWithFallback(file.toPath(), f.toPath(), false);
        } finally {
            this.file = f;
        }
    }

    public void truncateTo(long offset) throws IOException {
        OptionalLong newLastOffset = OptionalLong.empty();
        for (AbortedTxnWithPosition txnWithPosition : iterable()) {
            AbortedTxn abortedTxn = txnWithPosition.txn;
            if (abortedTxn.lastOffset() >= offset) {
                channel().truncate(txnWithPosition.position);
                lastOffset = newLastOffset;
                return;
            }
            newLastOffset = OptionalLong.of(abortedTxn.lastOffset());
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
        for (AbortedTxnWithPosition txnWithPosition : iterable()) {
            AbortedTxn abortedTxn = txnWithPosition.txn;
            if (abortedTxn.lastOffset() < startOffset)
                throw new CorruptIndexException("Last offset of aborted transaction " + abortedTxn + " in index "
                    + file.getAbsolutePath() + " is less than start offset " + startOffset);
        }
    }

    /**
     * Check if the index is empty.
     * @return `true` if the index is empty (or) when underlying file doesn't exists, `false` otherwise.
     */
    public boolean isEmpty() {
        return !iterable().iterator().hasNext();
    }

    private FileChannel openChannel() throws IOException {
        FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
        maybeChannel = Optional.of(channel);
        channel.position(channel.size());
        return channel;
    }

    private FileChannel channel() throws IOException {
        FileChannel channel = channelOrNull();
        if (channel == null)
            return openChannel();
        else
            return channel;
    }

    private FileChannel channelOrNull() {
        return maybeChannel.orElse(null);
    }

    private Iterable<AbortedTxnWithPosition> iterable() {
        FileChannel channel = channelOrNull();
        if (channel == null)
            return List.of();

        return () -> new Iterator<>() {
            private final ByteBuffer buffer = ByteBuffer.allocate(ABORTED_TXN_RECORD_SIZE);
            private int position = 0;

            @Override
            public boolean hasNext() {
                try {
                    return channel.position() - position >= ABORTED_TXN_RECORD_SIZE;
                } catch (IOException e) {
                    throw new KafkaException("Failed read position from the transaction index " + file.getAbsolutePath(), e);
                }
            }

            @Override
            public AbortedTxnWithPosition next() {
                try {
                    buffer.clear();
                    Utils.readFully(channel, buffer, position);
                    buffer.flip();

                    short version = buffer.getShort();
                    if (version < AbortedTxn.LOWEST_SUPPORTED_VERSION || version > AbortedTxn.HIGHEST_SUPPORTED_VERSION)
                        throw new KafkaException("Unexpected aborted transaction version " + version
                            + " in transaction index " + file.getAbsolutePath() + ", supported version range is "
                            + AbortedTxn.LOWEST_SUPPORTED_VERSION + " to " + AbortedTxn.HIGHEST_SUPPORTED_VERSION);
                    AbortedTxn abortedTxn = new AbortedTxn(new ByteBufferAccessor(buffer), version);
                    AbortedTxnWithPosition nextEntry = new AbortedTxnWithPosition(abortedTxn, position);
                    position += ABORTED_TXN_RECORD_SIZE;
                    return nextEntry;
                } catch (IOException e) {
                    // We received an unexpected error reading from the index file. We propagate this as an
                    // UNKNOWN error to the consumer, which will cause it to retry the fetch.
                    throw new KafkaException("Failed to read from the transaction index " + file.getAbsolutePath(), e);
                }
            }

        };
    }

}
