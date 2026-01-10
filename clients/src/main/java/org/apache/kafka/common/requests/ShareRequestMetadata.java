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

import org.apache.kafka.common.Uuid;

public class ShareRequestMetadata {
    /**
     * The first epoch. When used in a ShareFetch request, indicates that the client
     * wants to create a session.
     */
    public static final int INITIAL_EPOCH = 0;

    /**
     * An invalid epoch. When used in a ShareFetch request, indicates that the client
     * wants to close an existing session.
     */
    public static final int FINAL_EPOCH = -1;

    /**
     *
     */
    public boolean isNewSession() {
        return epoch == INITIAL_EPOCH;
    }

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
     * The member ID.
     */
    private final Uuid memberId;

    /**
     * The share session epoch.
     */
    private final int epoch;

    public ShareRequestMetadata(Uuid memberId, int epoch) {
        this.memberId = memberId;
        this.epoch = epoch;
    }

    public static ShareRequestMetadata initialEpoch(Uuid memberId) {
        return new ShareRequestMetadata(memberId, INITIAL_EPOCH);
    }

    public ShareRequestMetadata nextEpoch() {
        return new ShareRequestMetadata(memberId, nextEpoch(epoch));
    }

    public ShareRequestMetadata nextCloseExistingAttemptNew() {
        return new ShareRequestMetadata(memberId, INITIAL_EPOCH);
    }

    public ShareRequestMetadata finalEpoch() {
        return new ShareRequestMetadata(memberId, FINAL_EPOCH);
    }

    public Uuid memberId() {
        return memberId;
    }

    public int epoch() {
        return epoch;
    }

    public boolean isFinalEpoch() {
        return epoch == FINAL_EPOCH;
    }

    public String toString() {
        StringBuilder bld = new StringBuilder();
        bld.append("(memberId=").append(memberId).append(", ");
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