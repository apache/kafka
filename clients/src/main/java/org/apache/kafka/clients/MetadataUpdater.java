/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.kafka.clients;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.types.Struct;

import java.util.List;

/**
 * The interface used by `NetworkClient` to request cluster metadata info to be updated and to retrieve the cluster nodes
 * from such metadata. This is an internal class.
 * <p>
 * This class is not thread-safe!
 */
interface MetadataUpdater {

    /**
     * Gets the current cluster info without blocking.
     * 非阻塞的方式得到当前集群信息
     */
    List<Node> fetchNodes();

    /**
     * 如果一个修改到集群的元数据是过期的返回true
     * Returns true if an update to the cluster metadata info is due.
     */
    boolean isUpdateDue(long now);

    /**
     *
     * 如果需要和可能启动群集元数据更新。 返回时间，直到元数据更新（如果更新已经开始为这个调用的结果这将是0）
     * Starts a cluster metadata update if needed and possible. Returns the time until the metadata update (which would
     * be 0 if an update has been started as a result of this call).
     *
     * 如果实现依赖于NetworkClient发送请求，完成接收将通过maybeHandleCompletedReceive方法
     * If the implementation relies on `NetworkClient` to send requests, the completed receive will be passed to
     * `maybeHandleCompletedReceive`.
     *
     * “需要”和“可能”的语义与实现有关，并且可以考虑许多因素，例如节点可用性，自上次元数据更新以来的时间等等。
     * The semantics of `needed` and `possible` are implementation-dependent and may take into account a number of
     * factors like node availability, how long since the last metadata update, etc.
     */
    long maybeUpdate(long now);

    /**
     * 如果`request`是一个元数据请求，处理它并且返回true，反正返回false
     * If `request` is a metadata request, handles it and return `true`. Otherwise, returns `false`.
     *
     * 这为“ MetadataUpdater”实现提供了一种机制，可将NetworkClient实例用于其自身的请求，并通过特殊处理来断开此类请求。
     * This provides a mechanism for the `MetadataUpdater` implementation to use the NetworkClient instance for its own
     * requests with special handling for disconnections of such requests.
     */
    boolean maybeHandleDisconnection(ClientRequest request);

    /**
     * If `request` is a metadata request, handles it and returns `true`. Otherwise, returns `false`.
     *
     * This provides a mechanism for the `MetadataUpdater` implementation to use the NetworkClient instance for its own
     * requests with special handling for completed receives of such requests.
     */
    boolean maybeHandleCompletedReceive(ClientRequest request, long now, Struct body);

    /**
     * 定时更新当前集群的元数据信息  随后调用maybeUpdate将会触发开始修改 如果可能
     * Schedules an update of the current cluster metadata info. A subsequent call to `maybeUpdate` would trigger the
     * start of the update if possible (see `maybeUpdate` for more information).
     */
    void requestUpdate();
}
