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

package org.apache.kafka.image.node;

import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.node.printer.MetadataNodePrinter;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;


public class ProvenanceNode implements MetadataNode {
    /**
     * The name of this node.
     */
    public final static String NAME = "provenance";

    /**
     * The metadata provenance.
     */
    private final MetadataProvenance provenance;

    public ProvenanceNode(MetadataProvenance provenance) {
        this.provenance = provenance;
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public void print(MetadataNodePrinter printer) {
        ZonedDateTime zonedDateTime =
            Instant.ofEpochMilli(provenance.lastContainedLogTimeMs()).atZone(ZoneId.of("UTC"));
        printer.output("offset " + provenance.lastContainedOffset() +
                ", epoch " + provenance.lastContainedEpoch() +
                ", time " + DateTimeFormatter.ISO_ZONED_DATE_TIME.format(zonedDateTime));
    }
}
