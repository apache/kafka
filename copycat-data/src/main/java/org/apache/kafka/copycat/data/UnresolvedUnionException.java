/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/



package org.apache.kafka.copycat.data;

/** Thrown when the expected contents of a union cannot be resolved. */
public class UnresolvedUnionException extends DataRuntimeException {
    private Object unresolvedDatum;
    private Schema unionSchema;

    public UnresolvedUnionException(Schema unionSchema, Object unresolvedDatum) {
        super("Not in union " + unionSchema + ": " + unresolvedDatum);
        this.unionSchema = unionSchema;
        this.unresolvedDatum = unresolvedDatum;
    }

    public Object getUnresolvedDatum() {
        return unresolvedDatum;
    }

    public Schema getUnionSchema() {
        return unionSchema;
    }
}
