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

import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * A hash table used for de-duplicating the log. This hash table uses a cryptographically secure hash of the key as a proxy for the key
 * for comparisons and to save space on object overhead. Collisions are resolved by probing. This hash table does not support deletes.
 */
public class SkimpyOffsetMap implements OffsetMap {

    /**
     * The number of bytes of space each entry uses (the number of bytes in the hash plus an 8 byte offset)
     */
    public final int bytesPerEntry;

    private final ByteBuffer bytes;

    /* the hash algorithm instance to use */
    private final MessageDigest digest;

    /* the number of bytes for this hash algorithm */
    private final int hashSize;

    /**
     * The maximum number of entries this map can contain
     */
    private final int slots;

    /* cache some hash buffers to avoid reallocating each time */
    private final byte[] hash1;
    private final byte[] hash2;

    /* number of entries put into the map */
    private int entries = 0;

    /* number of lookups on the map */
    private long lookups = 0L;

    /* the number of probes for all lookups */
    private long probes = 0L;

    /* the latest offset written into the map */
    private long lastOffset = -1L;

    /**
     * Create an instance of SkimplyOffsetMap with the default hash algorithm (MD5).
     *
     * @param memory The amount of memory this map can use
     */
    public SkimpyOffsetMap(int memory) throws NoSuchAlgorithmException {
        this(memory, "MD5");
    }

    /**
     * Create an instance of SkimpyOffsetMap.
     *
     * @param memory The amount of memory this map can use
     * @param hashAlgorithm The hash algorithm instance to use: MD2, MD5, SHA-1, SHA-256, SHA-384, SHA-512
     */
    public SkimpyOffsetMap(int memory, String hashAlgorithm) throws NoSuchAlgorithmException {
        this.bytes = ByteBuffer.allocate(memory);

        this.digest = MessageDigest.getInstance(hashAlgorithm);

        this.hashSize = digest.getDigestLength();
        this.bytesPerEntry = hashSize + 8;
        this.slots = memory / bytesPerEntry;

        this.hash1 = new byte[hashSize];
        this.hash2 = new byte[hashSize];
    }

    @Override
    public int slots() {
        return slots;
    }

    /**
     * Get the offset associated with this key.
     * @param key The key
     * @return The offset associated with this key or -1 if the key is not found
     */
    @Override
    public long get(ByteBuffer key) throws DigestException {
        ++lookups;
        hashInto(key, hash1);
        // search for the hash of this key by repeated probing until we find the hash we are looking for or we find an empty slot
        int attempt = 0;
        int pos = 0;
        //we need to guard against attempt integer overflow if the map is full
        //limit attempt to number of slots once positionOf(..) enters linear search mode
        int maxAttempts = slots + hashSize - 4;
        do {
            if (attempt >= maxAttempts)
                return -1L;
            pos = positionOf(hash1, attempt);
            bytes.position(pos);
            if (isEmpty(pos))
                return -1L;
            bytes.get(hash2);
            ++attempt;
        } while (!Arrays.equals(hash1, hash2));
        return bytes.getLong();
    }

    /**
     * Associate this offset to the given key.
     * @param key The key
     * @param offset The offset
     */
    @Override
    public void put(ByteBuffer key, long offset) throws DigestException {
        if (entries >= slots)
            throw new IllegalArgumentException("Attempted to add a new entry to a full offset map, "
                + "entries: " + entries + ", slots: " + slots);

        ++lookups;
        hashInto(key, hash1);

        // probe until we find the first empty slot
        int attempt = 0;
        int pos = positionOf(hash1, attempt);
        while (!isEmpty(pos)) {
            bytes.position(pos);
            bytes.get(hash2);
            if (Arrays.equals(hash1, hash2)) {
                // we found an existing entry, overwrite it and return (size does not change)
                bytes.putLong(offset);
                lastOffset = offset;
                return;
            }
            ++attempt;
            pos = positionOf(hash1, attempt);
        }

        // found an empty slot, update it - size grows by 1
        bytes.position(pos);
        bytes.put(hash1);
        bytes.putLong(offset);
        lastOffset = offset;
        ++entries;
    }

    @Override
    public void updateLatestOffset(long offset) {
        this.lastOffset = offset;
    }

    /**
     * Change the salt used for key hashing making all existing keys unfindable.
     */
    @Override
    public void clear() {
        this.entries = 0;
        this.lookups = 0L;
        this.probes = 0L;
        this.lastOffset = -1L;
        Arrays.fill(bytes.array(), bytes.arrayOffset(), bytes.arrayOffset() + bytes.limit(), (byte) 0);
    }

    /**
     * The number of entries put into the map (note that not all may remain)
     */
    @Override
    public int size() {
        return entries;
    }

    /**
     * The latest offset put into the map
     */
    @Override
    public long latestOffset() {
        return lastOffset;
    }

    /**
     * The rate of collisions in the lookups
     */
    // Visible for testing
    public double collisionRate() {
        return (this.probes - this.lookups) / (double) this.lookups;
    }

    /**
     * Check that there is no entry at the given position
     */
    private boolean isEmpty(int position) {
        return bytes.getLong(position) == 0
            && bytes.getLong(position + 8) == 0
            && bytes.getLong(position + 16) == 0;
    }

    /**
     * Calculate the ith probe position. We first try reading successive integers from the hash itself
     * then if all of those fail we degrade to linear probing.
     * @param hash The hash of the key to find the position for
     * @param attempt The ith probe
     * @return The byte offset in the buffer at which the ith probing for the given hash would reside
     */
    private int positionOf(byte[] hash, int attempt) {
        int probe = ByteUtils.readIntBE(hash, Math.min(attempt, hashSize - 4)) + Math.max(0, attempt - hashSize + 4);
        int slot = Utils.abs(probe) % slots;
        ++this.probes;
        return slot * bytesPerEntry;
    }

    /**
     * The offset at which we have stored the given key
     * @param key The key to hash
     * @param buffer The buffer to store the hash into
     */
    private void hashInto(ByteBuffer key, byte[] buffer) throws DigestException {
        key.mark();
        digest.update(key);
        key.reset();
        digest.digest(buffer, 0, hashSize);
    }
}
