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
package kafka.server;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;

public abstract class ShareFetchContext {

    protected Logger log = LoggerFactory.getLogger(ShareFetchContext.class);

    String partitionsToLogString(Collection<TopicIdPartition> partitions) {
        return FetchSession.partitionsToLogString(partitions, log.isTraceEnabled());
    }

    /**
     * Return an empty throttled response due to quota violation.
     */
    ShareFetchResponse throttleResponse(int throttleTimeMs) {
        return new ShareFetchResponse(ShareFetchResponse.toMessage(Errors.NONE, throttleTimeMs,
                Collections.emptyIterator(), Collections.emptyList()));
    }

    /**
     * Get the response size to be used for quota computation. Since we are returning an empty response in case of
     * throttling, we are not supposed to update the context until we know that we are not going to throttle.
     */
    abstract int responseSize(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates,
                              short version);

    /**
     * Updates the share fetch context with new partition information. Generates response data.
     * The response data may require subsequent down-conversion.
     */
    abstract ShareFetchResponse updateAndGenerateResponseData(String groupId, Uuid memberId, LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates);

    abstract SharePartitionManager.ErroneousAndValidPartitionData getErroneousAndValidTopicIdPartitions();

}
