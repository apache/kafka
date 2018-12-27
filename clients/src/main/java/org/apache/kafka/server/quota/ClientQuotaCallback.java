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
package org.apache.kafka.server.quota;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.Map;

/**
 * Quota callback interface for brokers that enables customization of client quota computation.
 */
public interface ClientQuotaCallback extends Configurable {

    /**
     * Quota callback invoked to determine the quota metric tags to be applied for a request.
     * Quota limits are associated with quota metrics and all clients which use the same
     * metric tags share the quota limit.
     *
     * @param quotaType Type of quota requested
     * @param principal The user principal of the connection for which quota is requested
     * @param clientId  The client id associated with the request
     * @return quota metric tags that indicate which other clients share this quota
     */
    Map<String, String> quotaMetricTags(ClientQuotaType quotaType, KafkaPrincipal principal, String clientId);

    /**
     * Returns the quota limit associated with the provided metric tags. These tags were returned from
     * a previous call to {@link #quotaMetricTags(ClientQuotaType, KafkaPrincipal, String)}. This method is
     * invoked by quota managers to obtain the current quota limit applied to a metric when the first request
     * using these tags is processed. It is also invoked after a quota update or cluster metadata change.
     * If the tags are no longer in use after the update, (e.g. this is a {user, client-id} quota metric
     * and the quota now in use is a {user} quota), null is returned.
     *
     * @param quotaType  Type of quota requested
     * @param metricTags Metric tags for a quota metric of type `quotaType`
     * @return the quota limit for the provided metric tags or null if the metric tags are no longer in use
     */
    Double quotaLimit(ClientQuotaType quotaType, Map<String, String> metricTags);

    /**
     * Quota configuration update callback that is invoked when quota configuration for an entity is
     * updated in ZooKeeper. This is useful to track configured quotas if built-in quota configuration
     * tools are used for quota management.
     *
     * @param quotaType   Type of quota being updated
     * @param quotaEntity The quota entity for which quota is being updated
     * @param newValue    The new quota value
     */
    void updateQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity, double newValue);

    /**
     * Quota configuration removal callback that is invoked when quota configuration for an entity is
     * removed in ZooKeeper. This is useful to track configured quotas if built-in quota configuration
     * tools are used for quota management.
     *
     * @param quotaType   Type of quota being updated
     * @param quotaEntity The quota entity for which quota is being updated
     */
    void removeQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity);

    /**
     * Returns true if any of the existing quota configs may have been updated since the last call
     * to this method for the provided quota type. Quota updates as a result of calls to
     * {@link #updateClusterMetadata(Cluster)}, {@link #updateQuota(ClientQuotaType, ClientQuotaEntity, double)}
     * and {@link #removeQuota(ClientQuotaType, ClientQuotaEntity)} are automatically processed.
     * So callbacks that rely only on built-in quota configuration tools always return false. Quota callbacks
     * with external quota configuration or custom reconfigurable quota configs that affect quota limits must
     * return true if existing metric configs may need to be updated. This method is invoked on every request
     * and hence is expected to be handled by callbacks as a simple flag that is updated when quotas change.
     *
     * @param quotaType Type of quota
     */
    boolean quotaResetRequired(ClientQuotaType quotaType);

    /**
     * Metadata update callback that is invoked whenever UpdateMetadata request is received from
     * the controller. This is useful if quota computation takes partitions into account.
     * Topics that are being deleted will not be included in `cluster`.
     *
     * @param cluster Cluster metadata including partitions and their leaders if known
     * @return true if quotas have changed and metric configs may need to be updated
     */
    boolean updateClusterMetadata(Cluster cluster);

    /**
     * Closes this instance.
     */
    void close();
}

