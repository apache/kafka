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

package org.apache.kafka.server.group.share;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * This interface introduces methods which can be used by callers to interact with the
 * persistence impl responsible for storing share group/partition states.
 * For KIP-932, the persistence impl would be a share coordinator which will store information in
 * an internal topic. But this allows for other variations as well.
 */
@InterfaceStability.Evolving
public interface Persister {
  /**
   * Initialize the share partition state.
   *
   * @param request InitializeShareGroupStateParameters
   * @return InitializeShareGroupStateResult
   */
  CompletableFuture<List<InitializeShareGroupStateResult>> initializeState(InitializeShareGroupStateParameters request);

  /**
   * Read share-partition state from a persistence impl.
   *
   * @param request ReadShareGroupStateParameters
   * @return ReadShareGroupStateResult
   */
  CompletableFuture<List<ReadShareGroupStateResult>> readState(ReadShareGroupStateParameters request);

  /**
   * Write share-partition state to a persistence impl.
   *
   * @param request WriteShareGroupStateParameters
   * @return WriteShareGroupStateResult
   */
  CompletableFuture<List<WriteShareGroupStateResult>> writeState(WriteShareGroupStateParameters request);

  /**
   * Delete share-partition state from a persistence impl.
   *
   * @param request DeleteShareGroupStateParameters
   * @return DeleteShareGroupStateResult
   */
  CompletableFuture<List<DeleteShareGroupStateResult>> deleteState(DeleteShareGroupStateParameters request);

  /**
   * Read the offset information from share-partition state from a persistence impl.
   *
   * @param request ReadShareGroupOffsetsStateParameters
   * @return ReadShareGroupOffsetsStateResult
   */
  CompletableFuture<List<ReadShareGroupOffsetsStateResult>> readOffsets(ReadShareGroupOffsetsStateParameters request);
}
