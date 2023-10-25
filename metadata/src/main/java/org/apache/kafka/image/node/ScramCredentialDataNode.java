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

import org.apache.kafka.image.node.printer.MetadataNodePrinter;
import org.apache.kafka.metadata.ScramCredentialData;


public class ScramCredentialDataNode implements MetadataNode {
    private final ScramCredentialData data;

    public ScramCredentialDataNode(ScramCredentialData data) {
        this.data = data;
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    private static void arrayToHex(byte[] array, StringBuilder bld) {
        for (int i = 0; i < array.length; i++) {
            bld.append(String.format("%02x", array[i] & 0xff));
        }
    }

    @Override
    public void print(MetadataNodePrinter printer) {
        StringBuilder bld = new StringBuilder();
        bld.append("ScramCredentialData");
        bld.append("(salt=");
        if (printer.redactionCriteria().shouldRedactScram()) {
            bld.append("[redacted]");
        } else {
            arrayToHex(data.salt(), bld);
        }
        bld.append(", storedKey=");
        if (printer.redactionCriteria().shouldRedactScram()) {
            bld.append("[redacted]");
        } else {
            arrayToHex(data.storedKey(), bld);
        }
        bld.append(", serverKey=");
        if (printer.redactionCriteria().shouldRedactScram()) {
            bld.append("[redacted]");
        } else {
            arrayToHex(data.serverKey(), bld);
        }
        bld.append(", iterations=");
        if (printer.redactionCriteria().shouldRedactScram()) {
            bld.append("[redacted]");
        } else {
            bld.append(data.iterations());
        }
        bld.append(")");
        printer.output(bld.toString());
    }
}
