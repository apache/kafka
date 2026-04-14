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

package org.apache.kafka.connect.openlineage;

import org.apache.kafka.connect.openlineage.visitor.AzureBlobSinkVisitor;
import org.apache.kafka.connect.openlineage.visitor.BigQuerySinkVisitor;
import org.apache.kafka.connect.openlineage.visitor.CassandraSinkVisitor;
import org.apache.kafka.connect.openlineage.visitor.DebeziumVisitor;
import org.apache.kafka.connect.openlineage.visitor.ElasticsearchSinkVisitor;
import org.apache.kafka.connect.openlineage.visitor.GcsSinkVisitor;
import org.apache.kafka.connect.openlineage.visitor.GenericVisitor;
import org.apache.kafka.connect.openlineage.visitor.HdfsSinkVisitor;
import org.apache.kafka.connect.openlineage.visitor.HttpSinkVisitor;
import org.apache.kafka.connect.openlineage.visitor.JdbcSinkVisitor;
import org.apache.kafka.connect.openlineage.visitor.JdbcSourceVisitor;
import org.apache.kafka.connect.openlineage.visitor.MirrorMakerVisitor;
import org.apache.kafka.connect.openlineage.visitor.MongoDbSinkVisitor;
import org.apache.kafka.connect.openlineage.visitor.MongoDbSourceVisitor;
import org.apache.kafka.connect.openlineage.visitor.RedshiftSinkVisitor;
import org.apache.kafka.connect.openlineage.visitor.S3SinkVisitor;
import org.apache.kafka.connect.openlineage.visitor.SnowflakeSinkVisitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Maintains an ordered list of {@link ConnectorVisitor} implementations and
 * dispatches to the first one that matches a given connector configuration.
 * The {@link GenericVisitor} is always registered last as a fallback.
 */
public final class VisitorRegistry {

    private final List<ConnectorVisitor> visitors;

    public VisitorRegistry() {
        List<ConnectorVisitor> list = new ArrayList<>();
        list.add(new JdbcSourceVisitor());
        list.add(new JdbcSinkVisitor());
        list.add(new S3SinkVisitor());
        list.add(new GcsSinkVisitor());
        list.add(new AzureBlobSinkVisitor());
        list.add(new HdfsSinkVisitor());
        list.add(new DebeziumVisitor());
        list.add(new MongoDbSourceVisitor());
        list.add(new MongoDbSinkVisitor());
        list.add(new ElasticsearchSinkVisitor());
        list.add(new BigQuerySinkVisitor());
        list.add(new SnowflakeSinkVisitor());
        list.add(new CassandraSinkVisitor());
        list.add(new RedshiftSinkVisitor());
        list.add(new MirrorMakerVisitor());
        list.add(new HttpSinkVisitor());
        // GenericVisitor is the catch-all fallback and must be last
        list.add(new GenericVisitor());
        this.visitors = Collections.unmodifiableList(list);
    }

    /**
     * Find the first visitor that matches the connector configuration and
     * extract lineage information.
     *
     * @param config the connector configuration
     * @return extracted lineage; never {@code null}
     */
    public ConnectorLineage extractLineage(Map<String, String> config) {
        for (ConnectorVisitor visitor : visitors) {
            if (visitor.matches(config)) {
                return visitor.visit(config);
            }
        }
        // Should not happen because GenericVisitor matches everything
        return new GenericVisitor().visit(config);
    }

    /**
     * Returns the registered visitors in priority order.  Visible for
     * testing.
     */
    List<ConnectorVisitor> visitors() {
        return visitors;
    }
}
