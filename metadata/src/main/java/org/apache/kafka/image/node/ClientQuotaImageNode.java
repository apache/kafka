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

import org.apache.kafka.image.ClientQuotaImage;

import java.util.Collection;


public class ClientQuotaImageNode implements MetadataNode {
    /**
     * The client quota image.
     */
    private final ClientQuotaImage image;

    public ClientQuotaImageNode(ClientQuotaImage image) {
        this.image = image;
    }

    @Override
    public Collection<String> childNames() {
        return image.quotaMap().keySet();
    }

    @Override
    public MetadataNode child(String name) {
        Double result = image.quotaMap().get(name);
        if (result == null) return null;
        return new MetadataLeafNode(result + "");
    }
}
