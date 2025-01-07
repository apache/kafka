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
package org.apache.kafka.tiered.storage.specs;

import java.util.Objects;

public class RemoteFetchCount {
    private FetchCountAndOp segmentFetchCountAndOp;
    private FetchCountAndOp offsetIdxFetchCountAndOp = new FetchCountAndOp(-1);
    private FetchCountAndOp timeIdxFetchCountAndOp = new FetchCountAndOp(-1);
    private FetchCountAndOp txnIdxFetchCountAndOp = new FetchCountAndOp(-1);

    public RemoteFetchCount(int segmentFetchCountAndOp) {
        this.segmentFetchCountAndOp = new FetchCountAndOp(segmentFetchCountAndOp);
    }

    public RemoteFetchCount(int segmentFetchCountAndOp,
                            int offsetIdxFetchCountAndOp,
                            int timeIdxFetchCountAndOp,
                            int txnIdxFetchCountAndOp) {
        this.segmentFetchCountAndOp = new FetchCountAndOp(segmentFetchCountAndOp);
        this.offsetIdxFetchCountAndOp = new FetchCountAndOp(offsetIdxFetchCountAndOp);
        this.timeIdxFetchCountAndOp = new FetchCountAndOp(timeIdxFetchCountAndOp);
        this.txnIdxFetchCountAndOp = new FetchCountAndOp(txnIdxFetchCountAndOp);
    }

    public RemoteFetchCount(FetchCountAndOp segmentFetchCountAndOp,
                            FetchCountAndOp offsetIdxFetchCountAndOp,
                            FetchCountAndOp timeIdxFetchCountAndOp,
                            FetchCountAndOp txnIdxFetchCountAndOp) {
        this.segmentFetchCountAndOp = segmentFetchCountAndOp;
        this.offsetIdxFetchCountAndOp = offsetIdxFetchCountAndOp;
        this.timeIdxFetchCountAndOp = timeIdxFetchCountAndOp;
        this.txnIdxFetchCountAndOp = txnIdxFetchCountAndOp;
    }

    public FetchCountAndOp getSegmentFetchCountAndOp() {
        return segmentFetchCountAndOp;
    }

    public void setSegmentFetchCountAndOp(FetchCountAndOp segmentFetchCountAndOp) {
        this.segmentFetchCountAndOp = segmentFetchCountAndOp;
    }

    public FetchCountAndOp getOffsetIdxFetchCountAndOp() {
        return offsetIdxFetchCountAndOp;
    }

    public void setOffsetIdxFetchCountAndOp(FetchCountAndOp offsetIdxFetchCountAndOp) {
        this.offsetIdxFetchCountAndOp = offsetIdxFetchCountAndOp;
    }

    public FetchCountAndOp getTimeIdxFetchCountAndOp() {
        return timeIdxFetchCountAndOp;
    }

    public void setTimeIdxFetchCountAndOp(FetchCountAndOp timeIdxFetchCountAndOp) {
        this.timeIdxFetchCountAndOp = timeIdxFetchCountAndOp;
    }

    public FetchCountAndOp getTxnIdxFetchCountAndOp() {
        return txnIdxFetchCountAndOp;
    }

    public void setTxnIdxFetchCountAndOp(FetchCountAndOp txnIdxFetchCountAndOp) {
        this.txnIdxFetchCountAndOp = txnIdxFetchCountAndOp;
    }

    @Override
    public String toString() {
        return "RemoteFetchCount{" +
                "segmentFetchCountAndOp=" + segmentFetchCountAndOp +
                ", offsetIdxFetchCountAndOp=" + offsetIdxFetchCountAndOp +
                ", timeIdxFetchCountAndOp=" + timeIdxFetchCountAndOp +
                ", txnIdxFetchCountAndOp=" + txnIdxFetchCountAndOp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteFetchCount that = (RemoteFetchCount) o;
        return Objects.equals(segmentFetchCountAndOp, that.segmentFetchCountAndOp) &&
                Objects.equals(offsetIdxFetchCountAndOp, that.offsetIdxFetchCountAndOp) &&
                Objects.equals(timeIdxFetchCountAndOp, that.timeIdxFetchCountAndOp) &&
                Objects.equals(txnIdxFetchCountAndOp, that.txnIdxFetchCountAndOp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(segmentFetchCountAndOp, offsetIdxFetchCountAndOp, timeIdxFetchCountAndOp, txnIdxFetchCountAndOp);
    }

    public enum OperationType {
        EQUALS_TO,
        GREATER_THAN_OR_EQUALS_TO,
        LESS_THAN_OR_EQUALS_TO
    }

    public static class FetchCountAndOp {
        private final int count;
        private final OperationType operationType;

        public FetchCountAndOp(int count) {
            this.count = count;
            this.operationType = OperationType.EQUALS_TO;
        }

        public FetchCountAndOp(int count, OperationType operationType) {
            this.count = count;
            this.operationType = operationType;
        }

        public int getCount() {
            return count;
        }

        public OperationType getOperationType() {
            return operationType;
        }

        @Override
        public String toString() {
            return "FetchCountAndOp{" +
                    "count=" + count +
                    ", operationType=" + operationType +
                    '}';
        }
    }
}
