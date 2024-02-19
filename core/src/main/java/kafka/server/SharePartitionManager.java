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

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.message.ShareFetchResponseData;

import java.util.concurrent.CompletableFuture;

public class SharePartitionManager {

  private ReplicaManager replicaManager;

  public SharePartitionManager(ReplicaManager replicaManager) {
    this.replicaManager = replicaManager;
  }

  // TODO: Implement session. Initial implementation shall create new session on each call which can
  //  be used in further API calls.
  public ShareSession session() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public CompletableFuture<ShareFetchResponseData> fetchMessages(ShareSession session, PartitionInfo partitionInfo) {
    assert replicaManager != null;
    throw new UnsupportedOperationException("Not implemented yet");
  }

  // TODO: Define share session class.
  public static class ShareSession {

  }

}
