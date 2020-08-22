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
package org.apache.kafka.connect.mirror;

/** Directional pair of clustes, where source is replicated to target. */
public class SourceAndTarget {
    private String source;
    private String target;

    public SourceAndTarget(String source, String target) {
        this.source = source;
        this.target = target;
    }

    public String source() {
        return source;
    }

    public String target() {
        return target;
    }

    @Override
    public String toString() {
        return source + "->" + target;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        return other != null && toString().equals(other.toString());
    }
}

