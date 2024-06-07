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

import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.image.ScramImage;
import org.apache.kafka.metadata.ScramCredentialData;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;


public class ScramImageNode implements MetadataNode {
    /**
     * The name of this node.
     */
    public final static String NAME = "scram";

    /**
     * The SCRAM image.
     */
    private final ScramImage image;

    public ScramImageNode(ScramImage image) {
        this.image = image;
    }

    @Override
    public Collection<String> childNames() {
        ArrayList<String> childNames = new ArrayList<>();
        for (ScramMechanism mechanism : ScramMechanism.values()) {
            if (!mechanism.equals(ScramMechanism.UNKNOWN)) {
                childNames.add(mechanism.mechanismName());
            }
        }
        return childNames;
    }

    @Override
    public MetadataNode child(String name) {
        ScramMechanism mechanism = ScramMechanism.fromMechanismName(name);
        if (mechanism.equals(ScramMechanism.UNKNOWN)) return null;
        Map<String, ScramCredentialData> userData = image.mechanisms().get(mechanism);
        return new ScramMechanismNode(userData == null ? Collections.emptyMap() : userData);
    }
}
