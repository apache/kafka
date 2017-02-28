/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TransformationChain<R extends ConnectRecord<R>> {

    private final List<Transformation<R>> transformations;

    public TransformationChain(List<Transformation<R>> transformations) {
        this.transformations = transformations;
    }

    public R apply(R record) {
        if (transformations.isEmpty()) return record;

        for (Transformation<R> transformation : transformations) {
            record = transformation.apply(record);
            if (record == null) break;
        }

        return record;
    }

    public void close() {
        for (Transformation<R> transformation : transformations) {
            transformation.close();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransformationChain that = (TransformationChain) o;
        return Objects.equals(transformations, that.transformations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transformations);
    }

    public static <R extends ConnectRecord<R>> TransformationChain<R> noOp() {
        return new TransformationChain<R>(Collections.<Transformation<R>>emptyList());
    }

}
