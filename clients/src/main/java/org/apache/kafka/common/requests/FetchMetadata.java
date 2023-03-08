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

package org.apache.kafka.common.requests;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class FetchMetadata {
    public static final Logger log = LoggerFactory.getLogger(FetchMetadata.class);

    /**
     * The session ID used by clients with no session.
     */
    public static final int INVALID_SESSION_ID = 0;

    /**
     * The first epoch.  When used in a fetch request, indicates that the client
     * wants to create or recreate a session.
     */
    public static final int INITIAL_EPOCH = 0;

    /**
     * An invalid epoch.  When used in a fetch request, indicates that the client
     * wants to close any existing session, and not create a new one.
     */
    public static final int FINAL_EPOCH = -1;

    /**
     * The FetchMetadata that is used when initializing a new FetchSessionHandler.
     */
    public static final FetchMetadata INITIAL = new FetchMetadata(INVALID_SESSION_ID, INITIAL_EPOCH);

    /**
     * The FetchMetadata that is implicitly used for handling older FetchRequests that
     * don't include fetch metadata.
     */
    public static final FetchMetadata LEGACY = new FetchMetadata(INVALID_SESSION_ID, FINAL_EPOCH);

    /**
     * Returns the next epoch.
     *
     * @param prevEpoch The previous epoch.
     * @return          The next epoch.
     */
    public static int nextEpoch(int prevEpoch) {
        if (prevEpoch < 0) {
            // The next epoch after FINAL_EPOCH is always FINAL_EPOCH itself.
            return FINAL_EPOCH;
        } else if (prevEpoch == Integer.MAX_VALUE) {
            return 1;
        } else {
            return prevEpoch + 1;
        }
    }

    /**
     * The fetch session ID.
     */
    private final int sessionId;

    /**
     * The fetch session epoch.
     */
    private final int epoch;

    public FetchMetadata(int sessionId, int epoch) {
        this.sessionId = sessionId;
        this.epoch = epoch;
    }

    /**
     * Returns true if this is a full fetch request.
     */
    public boolean isFull() {
        return (this.epoch == INITIAL_EPOCH) || (this.epoch == FINAL_EPOCH);
    }

    public int sessionId() {
        return sessionId;
    }

    public int epoch() {
        return epoch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId, epoch);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FetchMetadata that = (FetchMetadata) o;
        return sessionId == that.sessionId && epoch == that.epoch;
    }

    /**
     * Return the metadata for the next request. The metadata is set to indicate that the client wants to close the
     * existing session.
     */
    public FetchMetadata nextCloseExisting() {
        return new FetchMetadata(sessionId, FINAL_EPOCH);
    }

    /**
     * Return the metadata for the next request. The metadata is set to indicate that the client wants to close the
     * existing session and create a new one if possible.
     */
    public FetchMetadata nextCloseExistingAttemptNew() {
        return new FetchMetadata(sessionId, INITIAL_EPOCH);
    }

    /**
     * Return the metadata for the next full fetch request.
     */
    public static FetchMetadata newIncremental(int sessionId) {
        return new FetchMetadata(sessionId, nextEpoch(INITIAL_EPOCH));
    }

    /**
     * Return the metadata for the next incremental response.
     */
    public FetchMetadata nextIncremental() {
        return new FetchMetadata(sessionId, nextEpoch(epoch));
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder();
        if (sessionId == INVALID_SESSION_ID) {
            bld.append("(sessionId=INVALID, ");
        } else {
            bld.append("(sessionId=").append(sessionId).append(", ");
        }
        if (epoch == INITIAL_EPOCH) {
            bld.append("epoch=INITIAL)");
        } else if (epoch == FINAL_EPOCH) {
            bld.append("epoch=FINAL)");
        } else {
            bld.append("epoch=").append(epoch).append(")");
        }
        return bld.toString();
    }
}
