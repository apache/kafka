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

package org.apache.kafka.server.share.context;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.server.share.ErroneousAndValidPartitionData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;

/**
 * The share fetch context for a final share fetch request.
 */
public class FinalContext extends ShareFetchContext {

    private static final Logger log = LoggerFactory.getLogger(FinalContext.class);

    public FinalContext() {
    }

    @Override
    boolean isTraceEnabled() {
        return log.isTraceEnabled();
    }

    @Override
    public int responseSize(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates, short version) {
        return ShareFetchResponse.sizeOf(version, updates.entrySet().iterator());
    }

    @Override
    public ShareFetchResponse updateAndGenerateResponseData(String groupId, Uuid memberId,
                                                     LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates) {
        log.debug("Final context returning {}", partitionsToLogString(updates.keySet()));
        return new ShareFetchResponse(ShareFetchResponse.toMessage(Errors.NONE, 0,
                updates.entrySet().iterator(), Collections.emptyList()));
    }

    @Override
    public ErroneousAndValidPartitionData getErroneousAndValidTopicIdPartitions() {
        return new ErroneousAndValidPartitionData();
    }
}
