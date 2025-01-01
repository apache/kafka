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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.coordinator.common.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecordSerde;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegularExpressionKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegularExpressionValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.generated.GroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupStatePartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupStatePartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataValue;

/**
 * Please ensure any new record added here stays in sync with DumpLogSegments.
 */
public class GroupCoordinatorRecordSerde extends CoordinatorRecordSerde {
    // This method is temporary until the share coordinator is converted to
    // using the new coordinator records.
    @Override
    public byte[] serializeKey(CoordinatorRecord record) {
        // Record does not accept a null key.
        return MessageUtil.toCoordinatorTypePrefixedBytes(
            record.key().version(),
            record.key().message()
        );
    }

    @Override
    protected ApiMessage apiMessageKeyFor(short recordVersion) {
        return switch (recordVersion) {
            case 0, 1 -> new OffsetCommitKey();
            case 2 -> new GroupMetadataKey();
            case 3 -> new ConsumerGroupMetadataKey();
            case 4 -> new ConsumerGroupPartitionMetadataKey();
            case 5 -> new ConsumerGroupMemberMetadataKey();
            case 6 -> new ConsumerGroupTargetAssignmentMetadataKey();
            case 7 -> new ConsumerGroupTargetAssignmentMemberKey();
            case 8 -> new ConsumerGroupCurrentMemberAssignmentKey();
            case 9 -> new ShareGroupPartitionMetadataKey();
            case 10 -> new ShareGroupMemberMetadataKey();
            case 11 -> new ShareGroupMetadataKey();
            case 12 -> new ShareGroupTargetAssignmentMetadataKey();
            case 13 -> new ShareGroupTargetAssignmentMemberKey();
            case 14 -> new ShareGroupCurrentMemberAssignmentKey();
            case 15 -> new ShareGroupStatePartitionMetadataKey();
            case 16 -> new ConsumerGroupRegularExpressionKey();
            default -> throw new CoordinatorLoader.UnknownRecordTypeException(recordVersion);
        };
    }

    @Override
    protected ApiMessage apiMessageValueFor(short recordVersion) {
        return switch (recordVersion) {
            case 0, 1 -> new OffsetCommitValue();
            case 2 -> new GroupMetadataValue();
            case 3 -> new ConsumerGroupMetadataValue();
            case 4 -> new ConsumerGroupPartitionMetadataValue();
            case 5 -> new ConsumerGroupMemberMetadataValue();
            case 6 -> new ConsumerGroupTargetAssignmentMetadataValue();
            case 7 -> new ConsumerGroupTargetAssignmentMemberValue();
            case 8 -> new ConsumerGroupCurrentMemberAssignmentValue();
            case 9 -> new ShareGroupPartitionMetadataValue();
            case 10 -> new ShareGroupMemberMetadataValue();
            case 11 -> new ShareGroupMetadataValue();
            case 12 -> new ShareGroupTargetAssignmentMetadataValue();
            case 13 -> new ShareGroupTargetAssignmentMemberValue();
            case 14 -> new ShareGroupCurrentMemberAssignmentValue();
            case 15 -> new ShareGroupStatePartitionMetadataValue();
            case 16 -> new ConsumerGroupRegularExpressionValue();
            default -> throw new CoordinatorLoader.UnknownRecordTypeException(recordVersion);
        };
    }
}
