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

package org.apache.kafka.coordinator.share;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecordSerde;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotKey;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotValue;
import org.apache.kafka.coordinator.share.generated.ShareUpdateKey;
import org.apache.kafka.coordinator.share.generated.ShareUpdateValue;

public class ShareCoordinatorRecordSerde extends CoordinatorRecordSerde {
    @Override
    protected ApiMessage apiMessageKeyFor(short recordVersion) {
        switch (recordVersion) {
            case ShareCoordinator.SHARE_SNAPSHOT_RECORD_KEY_VERSION:
                return new ShareSnapshotKey();
            case ShareCoordinator.SHARE_UPDATE_RECORD_KEY_VERSION:
                return new ShareUpdateKey();
            default:
                throw new CoordinatorLoader.UnknownRecordTypeException(recordVersion);
        }
    }

    @Override
    protected ApiMessage apiMessageValueFor(short recordVersion) {
        switch (recordVersion) {
            case ShareCoordinator.SHARE_SNAPSHOT_RECORD_VALUE_VERSION:
                return new ShareSnapshotValue();
            case ShareCoordinator.SHARE_UPDATE_RECORD_VALUE_VERSION:
                return new ShareUpdateValue();
            default:
                throw new CoordinatorLoader.UnknownRecordTypeException(recordVersion);
        }
    }
}
