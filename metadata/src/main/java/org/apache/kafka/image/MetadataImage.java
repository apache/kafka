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

package org.apache.kafka.image;

import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.raft.OffsetAndEpoch;

import java.util.Objects;


/**
 * The broker metadata image.
 *
 * This class is thread-safe.
 */
public final class MetadataImage {
    public final static MetadataImage EMPTY = new MetadataImage(
        MetadataProvenance.EMPTY,
        FeaturesImage.EMPTY,
        ClusterImage.EMPTY,
        TopicsImage.EMPTY,
        ConfigurationsImage.EMPTY,
        ClientQuotasImage.EMPTY,
        ProducerIdsImage.EMPTY,
        AclsImage.EMPTY);

    private final MetadataProvenance provenance;

    private final FeaturesImage features;

    private final ClusterImage cluster;

    private final TopicsImage topics;

    private final ConfigurationsImage configs;

    private final ClientQuotasImage clientQuotas;

    private final ProducerIdsImage producerIds;

    private final AclsImage acls;

    public MetadataImage(
        MetadataProvenance provenance,
        FeaturesImage features,
        ClusterImage cluster,
        TopicsImage topics,
        ConfigurationsImage configs,
        ClientQuotasImage clientQuotas,
        ProducerIdsImage producerIds,
        AclsImage acls
    ) {
        this.provenance = provenance;
        this.features = features;
        this.cluster = cluster;
        this.topics = topics;
        this.configs = configs;
        this.clientQuotas = clientQuotas;
        this.producerIds = producerIds;
        this.acls = acls;
    }

    public boolean isEmpty() {
        return features.isEmpty() &&
            cluster.isEmpty() &&
            topics.isEmpty() &&
            configs.isEmpty() &&
            clientQuotas.isEmpty() &&
            producerIds.isEmpty() &&
            acls.isEmpty();
    }

    public MetadataProvenance provenance() {
        return provenance;
    }

    public OffsetAndEpoch highestOffsetAndEpoch() {
        return new OffsetAndEpoch(provenance.offset(), provenance.epoch());
    }

    public long offset() {
        return provenance.offset();
    }

    public FeaturesImage features() {
        return features;
    }

    public ClusterImage cluster() {
        return cluster;
    }

    public TopicsImage topics() {
        return topics;
    }

    public ConfigurationsImage configs() {
        return configs;
    }

    public ClientQuotasImage clientQuotas() {
        return clientQuotas;
    }

    public ProducerIdsImage producerIds() {
        return producerIds;
    }

    public AclsImage acls() {
        return acls;
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        // Features should be written out first so we can include the metadata.version at the beginning of the
        // snapshot
        features.write(writer, options);
        cluster.write(writer, options);
        topics.write(writer, options);
        configs.write(writer, options);
        clientQuotas.write(writer, options);
        producerIds.write(writer, options);
        acls.write(writer, options);
        writer.close(true);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !o.getClass().equals(this.getClass())) return false;
        MetadataImage other = (MetadataImage) o;
        return provenance.equals(other.provenance) &&
            features.equals(other.features) &&
            cluster.equals(other.cluster) &&
            topics.equals(other.topics) &&
            configs.equals(other.configs) &&
            clientQuotas.equals(other.clientQuotas) &&
            producerIds.equals(other.producerIds) &&
            acls.equals(other.acls);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            provenance,
            features,
            cluster,
            topics,
            configs,
            clientQuotas,
            producerIds,
            acls);
    }

    @Override
    public String toString() {
        return "MetadataImage(" +
            "provenance=" + provenance +
            ", features=" + features +
            ", cluster=" + cluster +
            ", topics=" + topics +
            ", configs=" + configs +
            ", clientQuotas=" + clientQuotas +
            ", producerIdsImage=" + producerIds +
            ", acls=" + acls +
            ")";
    }
}
