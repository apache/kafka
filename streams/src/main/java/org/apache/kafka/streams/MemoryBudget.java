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
package org.apache.kafka.streams;

import java.util.concurrent.atomic.AtomicLong;

public class MemoryBudget {

    public enum AllocationType {
        SOFT, HARD
    }

    private static final ThreadLocal<Boolean> OVER_CAPACITY = ThreadLocal.withInitial(() -> false);
    private static final ThreadLocal<Long> THREAD_RESERVED = ThreadLocal.withInitial(() -> 0L);
    private static final ThreadLocal<Long> THREAD_FREE = ThreadLocal.withInitial(() -> 0L);

    // Allocate 16 times the amount we need (the power of two is to make it a shift)
    public static final int ALLOCATION_BLOCK = 16;

    // Free twice an allocation block (the power of two is to make it a shift)
    public static final int FREE_BLOCK = ALLOCATION_BLOCK * 2;

    private final AtomicLong totalBudget;
    private final AtomicLong hardAllocation;
    private final AtomicLong softAllocation;

    public MemoryBudget(final AtomicLong totalBudget) {
        this.totalBudget = totalBudget;
        hardAllocation = new AtomicLong(0);
        softAllocation = new AtomicLong(0);
    }

    public void transact(final AllocationType allocationType, final long sizeDelta) {
        switch (allocationType) {
            case HARD:
                hardTransact(sizeDelta);
                break;
            case SOFT:
                softTransact(sizeDelta);
                break;
        }
    }

    public void hardTransact(final long sizeDelta) {
        if (sizeDelta > 0) {
            hardAllocate(sizeDelta);
        } else if (sizeDelta < 0) {
            hardFree(-1 * sizeDelta);
        }
    }

    public void softTransact(final long sizeDelta) {
        if (sizeDelta > 0) {
            softAllocate(sizeDelta);
        } else if (sizeDelta < 0) {
            softFree(-1 * sizeDelta);
        }
    }

    private void softAllocate(final long size) {
        // should assert size is positive if this becomes pubic
        final long reserved = THREAD_RESERVED.get();
        final long free = THREAD_FREE.get();
        if (size <= free) {
            // If we have already requested enough memory, then just use some.
            THREAD_FREE.set(free - size);
        } else {
            // If we need more memory than this thread has requested,
            // then request an extra allocation block
            final long blockToReserve = size * ALLOCATION_BLOCK;
            final long soft = softAllocation.addAndGet(blockToReserve);
            final long hard = hardAllocation.get();
            final long totalBudget = this.totalBudget.get();
            OVER_CAPACITY.set((soft + hard) > totalBudget);

            THREAD_RESERVED.set(reserved + blockToReserve);
            // don't forget to take out the bite we need from the
            // block we requested
            THREAD_FREE.set(free + blockToReserve - size);
        }
    }

    private void hardAllocate(final long size) {
        // should assert size is positive if this becomes pubic
        final long reserved = THREAD_RESERVED.get();
        final long free = THREAD_FREE.get();
        if (size <= free) {
            // If we have already requested enough memory, then just use some.
            THREAD_FREE.set(free - size);
        } else {
            // If we need more memory than this thread has requested,
            // then request an extra allocation block
            final long blockToReserve = size * ALLOCATION_BLOCK;

            final long soft = softAllocation.get();
            final long hard = hardAllocation.addAndGet(blockToReserve);
            final long totalBudget = this.totalBudget.get();
            OVER_CAPACITY.set((soft + hard) > totalBudget);

            THREAD_RESERVED.set(reserved + blockToReserve);
            // don't forget to take out the bite we need from the
            // block we requested
            THREAD_FREE.set(free + blockToReserve - size);
        }
    }

    public void softFree(final long size) {
        if (size < 0) {
            throw new IllegalArgumentException("Nonsense free: " + size);
        }
        final Long reserved = THREAD_RESERVED.get();
        final Long free = THREAD_FREE.get();

        // If we can afford to give up a whole free block, then do it.
        final long blockToFree = size * FREE_BLOCK;
        if (blockToFree < free) {
            // release the block from the shared pool
            softAllocation.addAndGet(-1 * blockToFree);

            // reduce the reservation size
            THREAD_RESERVED.set(reserved - blockToFree);

            // reduce the size of the free pool
            THREAD_FREE.set(free - blockToFree);
        } else {
            // Otherwise, just free up the requested size
            THREAD_FREE.set(free + size);
        }
    }

    public void hardFree(final long size) {
        if (size < 0) {
            throw new IllegalArgumentException("Nonsense free: " + size);
        }
        final Long reserved = THREAD_RESERVED.get();
        final Long free = THREAD_FREE.get();

        // If we can afford to give up a whole free block, then do it.
        final long blockToFree = size * FREE_BLOCK;
        if (blockToFree < free) {
            // release the block from the shared pool
            hardAllocation.addAndGet(-1 * blockToFree);

            // reduce the reservation size
            THREAD_RESERVED.set(reserved - blockToFree);

            // reduce the size of the free pool
            THREAD_FREE.set(free - blockToFree);
        } else {
            // Otherwise, just free up the requested size
            THREAD_FREE.set(free + size);
        }
    }


    public void free(final AllocationType allocationType, final long size) {
        switch (allocationType) {
            case SOFT:
                softFree(size);
                break;
            case HARD:
                hardFree(size);
                break;
        }
    }

    public boolean overHardBudget() {
        return hardAllocation.get() > totalBudget.get();
    }

    public long targetSoftReservation() {
        final long hard = hardAllocation.get();
        final long soft = softAllocation.get();
        final long consumed = hard + soft;
        final long total = totalBudget.get();
        final long needToFree = consumed - total;
        if (needToFree <= 0L) {
            // If we don't need to free anything, then the target is the current reservation
            return THREAD_RESERVED.get();
        } else {
            // If we need to free some amount, we can free at most the soft reservation
            final Long reserved = THREAD_RESERVED.get();
            final long target = reserved - needToFree;
            return Math.max(target, 0L);
        }
    }

    public long softReservation() {
        return THREAD_RESERVED.get();
    }

    @Override
    public String toString() {
        return "MemoryBudget{" +
            "totalBudget=" + totalBudget +
            ", hardAllocation=" + hardAllocation +
            ", softAllocation=" + softAllocation +
            '}';
    }
}
