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

package org.apache.kafka.clients;

import com.ibm.disni.verbs.IbvMr;
import org.apache.kafka.clients.consumer.internals.ConsumerRDMAClient;

import org.apache.kafka.common.TopicPartition;


import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.RDMAConsumeAddressResponse;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.LinkedList;
import java.util.TreeSet;


class SlotSegment {
    public final TopicPartition tp;
    public final long baseOffset;
    public SlotSegment(TopicPartition tp, long baseOffset) {
        this.tp = tp;
        this.baseOffset = baseOffset;
    }

    @Override
    public int hashCode() {
        return (int) (31 * baseOffset + tp.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SlotSegment other = (SlotSegment) obj;
        return tp.equals(other.tp) && baseOffset == other.baseOffset;
    }

}


// frequencyOfRdmaUpdate
// withSlots
// slot buffer
// cacheSize

public class FetchRdmaSessionHandler {
    private final Logger log;
    private final long frequencyOfRdmaUpdate; // nanoseconds  = 1000
    private final int cacheSize;
    private final int wrapAroundLimit;
    private final long addressUpdateTimeoutInMs;

    private final int node;
    private final int fetchSize;
    private final ConsumerRDMAClient rdmaClient; // only for memory registration


    /*Here all values related to slot functionality*/
    private final boolean withSlots;
    private   ByteBuffer bufferForSlots;
    private   IbvMr slotMr;
    private   long slotAddress = -1;
    private   long slotEnd = -1;
    private   int slotRkey = -1;
    private   long lastRdmaUpdate = 0L;
    private   int hasPendingRdmaSlotRequest = 0;
    private LinkedHashMap<Long, SlotSegment> addressToSlotSegment =
            new LinkedHashMap<>(0);




    /**
     * All of the partitions which exist in the fetch request session.
     */
    private LinkedHashMap<TopicPartition, FetchRdmaRequestData> sessionPartitions =
            new LinkedHashMap<>(0);



    public FetchRdmaSessionHandler(LogContext logContext, int node, ConsumerRDMAClient rdmaClient,
                                   int fetchSize, int cacheSize, int wrapAroundLimit, boolean withSlots,
                                   long frequencyOfRdmaUpdate, long addressUpdateTimeoutInMs) {
        this.log = logContext.logger(FetchRdmaSessionHandler.class);
        this.node = node;
        this.rdmaClient = rdmaClient;
        this.bufferForSlots = ByteBuffer.allocateDirect(40 * 50);
        this.slotMr = null;
        this.fetchSize = fetchSize;
        this.withSlots = withSlots;
        this.cacheSize = cacheSize;
        this.wrapAroundLimit =  wrapAroundLimit;
        this.frequencyOfRdmaUpdate = frequencyOfRdmaUpdate;
        this.addressUpdateTimeoutInMs = addressUpdateTimeoutInMs;

    }


    public FetchRdmaRequestData handleResponse(ClientRDMAResponse response) {

        RDMAWrBuilder temp = response.getRequest();

        FetchRDMAReadRequest request = (FetchRDMAReadRequest) temp;
        TopicPartition tp = request.getTopicPartition();


        FetchRdmaRequestData handler = sessionPartitions.get(tp);

        CompletedRdmaFetch fetch = new CompletedRdmaFetch(request.remoteAddress, request.length, response.responseBody());
        handler.addFetch(fetch);

        return handler;
    }

    public void handleError(Exception e) {
        // empty
        System.out.println("Error" + e);
        // todo
    }


    public boolean requiresUpdatePartition(TopicPartition partition, IsolationLevel isolationLevel, long nowNanos) {
        if (!sessionPartitions.containsKey(partition)) {
            sessionPartitions.put(partition, 
                                  new FetchRdmaRequestData(partition, rdmaClient, nowNanos, fetchSize,
                                                           cacheSize, wrapAroundLimit, withSlots, addressUpdateTimeoutInMs));
            return true;
        }

        return sessionPartitions.get(partition).requiresUpdate(isolationLevel, nowNanos);
    }

    public long getBaseOffsetToForget(TopicPartition partition) {
        long toForget = sessionPartitions.get(partition).getBaseOffsetToForget();
        if (withSlots && toForget >= 0) {
            // TODO can be optimized
            addressToSlotSegment.values().remove(new SlotSegment(partition, toForget));
        }
        return toForget;
    }



    public boolean requiresRdmaUpdatePartition(TopicPartition tp, IsolationLevel isolationLevel,  long nowNanos) {

        boolean needUpdate = withSlots && (this.slotRkey != -1 && hasPendingRdmaSlotRequest == 0 &&
                             (nowNanos - lastRdmaUpdate > frequencyOfRdmaUpdate) &&
                             sessionPartitions.get(tp).requiresRdmaUpdate(isolationLevel));
        if (needUpdate) {
            lastRdmaUpdate = nowNanos;
        }
        return  needUpdate;
    }

    public RDMAWrBuilder getRdmaUpdateRequest() {
        if (!withSlots) {
            System.out.println("Error. Slots has been disabled but getRdmaUpdateRequest is called");
        }
        int length = (int) (slotEnd - slotAddress);
        if (this.slotMr == null) {
            this.slotMr = rdmaClient.MemReg(bufferForSlots);
        }
        hasPendingRdmaSlotRequest = 1;
        return new FetchRDMASlotRequest(slotAddress, slotRkey, length, bufferForSlots, slotMr.getLkey());
    }


    public boolean isReady(TopicPartition partition) {
        return sessionPartitions.get(partition).isReady();
    }


    public RDMAWrBuilder getFetchRequestForPartiton(TopicPartition partition, IsolationLevel isolationLevel, long offset) {
        return sessionPartitions.get(partition).getFetchRequest(isolationLevel);
    }


    public void updateAllMetadata(ClientRDMAResponse response) {
        hasPendingRdmaSlotRequest = 0;

        ByteBuffer buffer = response.responseBody();
        int length = response.GetLength();

        for (int offset = 0; offset < length; offset += 40) { // TODO 40 is SlotSize
            long address = slotAddress + offset;

            SlotSegment ss = addressToSlotSegment.getOrDefault(address, null);
            if (ss != null) {
                ByteBuffer slot = ((ByteBuffer) (buffer.duplicate().position(offset).limit(offset + 40))).slice(); // TODO 40 is SlotSize
                sessionPartitions.get(ss.tp).updateMetadata(ss.baseOffset, slot);
            }
        }
    }

    public void updateMetadata(Map<TopicPartition, RDMAConsumeAddressResponse.PartitionData> update) {

        for (Map.Entry<TopicPartition, RDMAConsumeAddressResponse.PartitionData> entry : update.entrySet()) {
            updateMetadata(entry.getKey(), entry.getValue());
        }
    }

    private void updateMetadata(TopicPartition partition, RDMAConsumeAddressResponse.PartitionData memoryData) {
        if (withSlots && memoryData.slotRkey != -1 && memoryData.slotAddress > 0L) {
            if (this.slotRkey == -1) {
                this.slotAddress = memoryData.slotAddress;
                this.slotEnd = memoryData.slotAddress + 40; // TODO 40 is SlotSize
                this.slotRkey = memoryData.slotRkey;
            } else {
                this.slotAddress = Math.min(memoryData.slotAddress, this.slotAddress);
                this.slotEnd = Math.max(memoryData.slotAddress + 40, this.slotEnd); // TODO 40 is SlotSize
                assert this.slotRkey == memoryData.slotRkey;
            }

            addressToSlotSegment.putIfAbsent(memoryData.slotAddress, new SlotSegment(partition, memoryData.baseOffset));

        }

        sessionPartitions.get(partition).updateMetadata(memoryData);
    }

    public static class CompletedRdmaFetch implements Comparable<CompletedRdmaFetch> {
        public final long startAddress;
        public final long length;
        public final ByteBuffer buffer; // for debugging

        public CompletedRdmaFetch(long startAddress, long length, ByteBuffer buffer) {
            this.startAddress = startAddress;
            this.length = length;
            this.buffer = buffer;
        }

        @Override
        public int compareTo(CompletedRdmaFetch other) {
            return Long.compare(startAddress, other.startAddress);
        }

    }


    public static class Segment {
        // offsets in Kafka terms

        public final long fetchBaseOffset;
        public final int rkey;
        public final long startAddress;


        public long currentOffsetPosition; // offset in bytes
        public long lastRequestedOffsetPosition; // offset in bytes
        public long currentHighWatermarkPosition = -1; // offset in bytes
        public long currentLastStablePosition; // offset in bytes
        public long currentWrittenPosition = -1; // offset in bytes

        public boolean sealed = false;


        public Segment(long baseOffset, long position, long startAddress, int rkey) {
            this.fetchBaseOffset = baseOffset;
            this.currentOffsetPosition = position;
            this.lastRequestedOffsetPosition = position;
            this.rkey = rkey;
            this.startAddress = startAddress;
        }
    }


    public static class FetchRdmaRequestData {

        private final boolean withSlots;
        private final int fetchSize;
        private final long addressUpdateTimeout; // 100 ms = 100_000_000L
        public final TopicPartition topicPartition;

        public long highWatermark = 0; // offset in Kafka terms
        public long lastStableOffset = 0; // offset in Kafka terms


        public short apiversion;


        public final LinkedList<Segment> toForget = new LinkedList<>();
        public Segment activeSegment = null;
        public Segment futureSegment = null;



        private boolean hasPendingAddressRequest = false;
        private long lastUpdateRequested;



        private final ConsumerRDMAClient rdmaClient;

        private final int cacheSize;

        private final int wrapAroundLimit; // once the size of buffer less than this number we wrap around

        private int cachePosition = 0;
        private int processedPosition = 0;
        ByteBuffer cache;
        IbvMr mr;


        // for debugging
        private int failuresToGetRecords = 0;
        private int pendingRDMAreq = 0;

        TreeSet<CompletedRdmaFetch> set = new TreeSet<>();


        FetchRdmaRequestData(TopicPartition topicPartition, ConsumerRDMAClient rdmaClient, long nowNanos,
                             int fetchSize, int cacheSize, int wrapAroundLimit, boolean withSlots, long addressUpdateTimeoutInMs) {
            this.fetchSize = fetchSize;
            this.lastUpdateRequested = nowNanos;

            this.topicPartition = topicPartition;

            this.rdmaClient = rdmaClient;
            this.mr = null;

            this.cacheSize = cacheSize;
            this.wrapAroundLimit = wrapAroundLimit;
            this.cache = ByteBuffer.allocateDirect(cacheSize);
            this.withSlots = withSlots;
            this.addressUpdateTimeout = addressUpdateTimeoutInMs * 1_000_000L; // convert to nano sec

          //  this.mr = rdmaClient.MemReg(targetBuffer); // we cannot allocate immediately since there could be no rdma ProtectionDomain
        }


        public void updateMetadata(RDMAConsumeAddressResponse.PartitionData memoryData) {

            hasPendingAddressRequest = false;

            Segment toUpdate = null;

            if (activeSegment != null) {
                if (activeSegment.fetchBaseOffset > memoryData.baseOffset) {
                    System.out.println("Received extremely old metadata update");
                    return;
                }

                if (activeSegment.fetchBaseOffset == memoryData.baseOffset) {
                    toUpdate = activeSegment;
                } else {

                    if (futureSegment != null) {
                        if (futureSegment.fetchBaseOffset == memoryData.baseOffset) {
                            System.out.println("we update futureSegment");
                            toUpdate = futureSegment;
                        } else {
                            System.out.println("unexpected segment baseoffset");
                        }
                    } else {
                        futureSegment = new Segment(memoryData.baseOffset, memoryData.position, memoryData.address, memoryData.rkey);
                        toUpdate = futureSegment;
                    }
                }
            } else {
                activeSegment = new Segment(memoryData.baseOffset, memoryData.position, memoryData.address, memoryData.rkey);
                toUpdate = activeSegment;
            }

            if (toUpdate == null) {
                return;
            }


            if (toUpdate.currentWrittenPosition < memoryData.writtenPosition || toUpdate.currentHighWatermarkPosition < memoryData.watermarkPosition) {
                //System.out.println("Update metadata");
                highWatermark = memoryData.watermarkOffset;
                lastStableOffset = memoryData.lastStableOffset;
                toUpdate.currentWrittenPosition = memoryData.writtenPosition;
                toUpdate.currentHighWatermarkPosition = memoryData.watermarkPosition;
                toUpdate.currentLastStablePosition = memoryData.lastStablePosition;
                toUpdate.sealed = memoryData.fileIsSealed;
            } else {
                System.out.println("No update required");
            }


            if (this.mr == null) {
                this.mr = rdmaClient.MemReg(cache);
            }
        }

        // from slots
        public void updateMetadata(long baseOffset, ByteBuffer buffer) {
            long slotSealed = buffer.getLong();
            long wmposition = buffer.getLong();
            long lsoposition =  buffer.getLong();
            long wmoffset =  buffer.getLong();
            long lsooffset =  buffer.getLong();

            Segment toUpdate = null;

            if (activeSegment != null) {
                if (activeSegment.fetchBaseOffset == baseOffset)
                    toUpdate = activeSegment;
            }

            if (futureSegment != null) {
                if (futureSegment.fetchBaseOffset == baseOffset)
                    toUpdate = futureSegment;
            }

            if (toUpdate != null) {
                toUpdate.currentHighWatermarkPosition = wmposition;
                toUpdate.currentLastStablePosition = lsoposition;

                highWatermark = wmoffset;
                lastStableOffset = lsooffset;

                if (slotSealed < 0) {
                    toUpdate.sealed = true;
                    toUpdate.currentWrittenPosition = -slotSealed;
                } else {
                    toUpdate.currentWrittenPosition = slotSealed;
                }
            }


        }

        public boolean isReady() {
            // we need to update if we are done with the current segment
            return activeSegment != null;
        }

        public long getBaseOffsetToForget() {
            return toForget.isEmpty() ? -1L : toForget.pollFirst().fetchBaseOffset;
        }

        public boolean requiresUpdate(IsolationLevel isolationLevel, long nowNanos) {
            // we need to update if we are done with the current segment
            // or the request on initial metadata has not been received


            boolean updateNeeded = (!isReady() || isExhausted(isolationLevel)) && (futureSegment == null);
            boolean updateShouldBeSend = !hasPendingAddressRequest || (nowNanos - lastUpdateRequested > addressUpdateTimeout);

            if (updateNeeded && updateShouldBeSend) {
                lastUpdateRequested = nowNanos;
                hasPendingAddressRequest = true;
                return true;
            }

            return false;
        }


        // TODO make use of isolationLevel
        public boolean requiresRdmaUpdate(IsolationLevel isolationLevel) {
            return withSlots && (activeSegment != null) && !activeSegment.sealed;
        }

        private boolean isExhausted(IsolationLevel isolationLevel) {

            if (activeSegment == null) {
                return true;
            }

            return (!withSlots || activeSegment.sealed) && (activeSegment.lastRequestedOffsetPosition == activeSegment.currentHighWatermarkPosition);
        }

        private boolean isCompleted() {
            return activeSegment.sealed && (activeSegment.currentOffsetPosition == activeSegment.currentHighWatermarkPosition);
        }



        public void addFetch(CompletedRdmaFetch fetch) {
            pendingRDMAreq--;
            set.add(fetch);
        }

        public RDMAWrBuilder getFetchRequest(IsolationLevel isolationLevel) {

            long maxFetchablePosition = activeSegment.currentHighWatermarkPosition;
            int length = (int) Math.min(maxFetchablePosition - activeSegment.lastRequestedOffsetPosition, cacheSize - cachePosition);

            if (length == 0)
                return null;
            else {

                length = Math.min(length, fetchSize);
                ByteBuffer targetBuffer = ((ByteBuffer) cache.duplicate().position(cachePosition).limit(cachePosition + length)).slice();

                FetchRDMAReadRequest req =
                        new FetchRDMAReadRequest(topicPartition, activeSegment.fetchBaseOffset,
                                activeSegment.startAddress + activeSegment.lastRequestedOffsetPosition,
                                activeSegment.rkey, length, targetBuffer, mr.getLkey());

                cachePosition += length;
                activeSegment.lastRequestedOffsetPosition += length;
                pendingRDMAreq++;

                return req;
            }

        }

        public MemoryRecords getMemoryRecords() {
            if (activeSegment == null) {
                System.out.println("Unexpected activeSegment == null");
            }

            long currentAddress = activeSegment.startAddress + activeSegment.currentOffsetPosition;
            int length = 0;

            while (!set.isEmpty() && set.first().startAddress == currentAddress) {
                CompletedRdmaFetch fetch = set.pollFirst();
                length += fetch.length;
                currentAddress += fetch.length;
            }

            if (length == 0) {
                // now new fetches or we are missing the first block
                return null;
            } else {

                ByteBuffer buffer = ((ByteBuffer) (cache.duplicate().position(processedPosition).limit(processedPosition + length))).slice();
                MemoryRecords rec = MemoryRecords.readableRecords(buffer);
                // We give out only readable records!
                int readablebytes = rec.validBytes();
                buffer.limit(readablebytes);
                ByteBuffer copyForUser = (ByteBuffer) ByteBuffer.allocate(readablebytes).put(buffer).rewind();

                activeSegment.currentOffsetPosition += readablebytes;
                processedPosition += readablebytes;

                // if we cropped a piece of buffer then we reinsert the rest
                if (readablebytes != length) {
                    int tempLength = length - readablebytes;
                    ByteBuffer temp = ((ByteBuffer) (cache.duplicate().position(processedPosition).limit(processedPosition + tempLength))).slice();
                    set.add(new CompletedRdmaFetch(activeSegment.startAddress + activeSegment.currentOffsetPosition, tempLength, temp));
                }

                // check whether we should wrap around cache
                int useableBytes = cacheSize - cachePosition;
                if (pendingRDMAreq == 0 && useableBytes < wrapAroundLimit) {
                    // perform wrapping around and copying unprocessed bytes.

                    // we will copy processibleBytes
                    int processibleBytes = cachePosition - processedPosition;
                    if (processibleBytes > 0) {
                        ByteBuffer copyFrom = ((ByteBuffer) (cache.duplicate().position(processedPosition).limit(processedPosition + processibleBytes))).slice();
                        cache.rewind();
                        cache.put(copyFrom);
                        cache.rewind();
                        set.clear();
                        ByteBuffer temp = ((ByteBuffer) (cache.duplicate().position(0).limit(processibleBytes))).slice();
                        set.add(new CompletedRdmaFetch(activeSegment.startAddress + activeSegment.currentOffsetPosition, processibleBytes, temp));
                    }

                    cachePosition = processibleBytes;
                    processedPosition = 0;
                }

                if (isCompleted()) {
                    assert set.isEmpty();
                    toForget.add(activeSegment);
                    activeSegment = futureSegment;
                    futureSegment = null;
                }

                if (readablebytes == 0) {
                    failuresToGetRecords++;
                    if (failuresToGetRecords > 100) {
                        System.out.println("Too many failures in parsing RDMA fetches. Increase fetchsize or code is broken");
                    }
                    return null;
                } else {
                    failuresToGetRecords = 0;
                    return MemoryRecords.readableRecords(copyForUser);
                }
            }
        }
    }
}
