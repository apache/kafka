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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.image.node.AclsImageNode;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAclWithId;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;


/**
 * Represents the ACLs in the metadata image.
 *
 * This class is thread-safe.
 */
public final class AclsImage {
    public static final AclsImage EMPTY = new AclsImage(Collections.emptyMap());

    private final Map<Uuid, StandardAcl> acls;

    public AclsImage(Map<Uuid, StandardAcl> acls) {
        this.acls = Collections.unmodifiableMap(acls);
    }

    public boolean isEmpty() {
        return acls.isEmpty();
    }

    public Map<Uuid, StandardAcl> acls() {
        return acls;
    }

    public void write(ImageWriter writer) {
        for (Entry<Uuid, StandardAcl> entry : acls.entrySet()) {
            StandardAclWithId aclWithId = new StandardAclWithId(entry.getKey(), entry.getValue());
            writer.write(0, aclWithId.toRecord());
        }
    }

    @Override
    public int hashCode() {
        return acls.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AclsImage other)) return false;
        return acls.equals(other.acls);
    }

    @Override
    public String toString() {
        return new AclsImageNode(this).stringify();
    }
}
