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
    private final FetchCountAndOp segmentFetchCountAndOp;
    private FetchCountAndOp offsetIdxFetchCountAndOp = new FetchCountAndOp(-1);
    private FetchCountAndOp timeIdxFetchCountAndOp = new FetchCountAndOp(-1);
    private FetchCountAndOp txnIdxFetchCountAndOp = new FetchCountAndOp(-1);

    public RemoteFetchCount(int segmentFetchCountAndOp) {
        this.segmentFetchCountAndOp = new FetchCountAndOp(segmentFetchCountAndOp);
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

    public FetchCountAndOp getOffsetIdxFetchCountAndOp() {
        return offsetIdxFetchCountAndOp;
    }

    public FetchCountAndOp getTimeIdxFetchCountAndOp() {
        return timeIdxFetchCountAndOp;
    }

    public FetchCountAndOp getTxnIdxFetchCountAndOp() {
        return txnIdxFetchCountAndOp;
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
}
