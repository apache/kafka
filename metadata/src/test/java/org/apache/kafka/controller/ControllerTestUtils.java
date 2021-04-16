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

package org.apache.kafka.controller;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.metadata.ApiMessageAndVersion;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class ControllerTestUtils {
    public static void replayAll(Object target,
                                 List<ApiMessageAndVersion> recordsAndVersions) throws Exception {
        for (ApiMessageAndVersion recordAndVersion : recordsAndVersions) {
            ApiMessage record = recordAndVersion.message();
            try {
                Method method = target.getClass().getMethod("replay", record.getClass());
                method.invoke(target, record);
            } catch (NoSuchMethodException e) {
                // ignore
            }
        }
    }

    public static <T> Set<T> iteratorToSet(Iterator<T> iterator) {
        HashSet<T> set = new HashSet<>();
        while (iterator.hasNext()) {
            set.add(iterator.next());
        }
        return set;
    }

    public static void assertBatchIteratorContains(List<List<ApiMessageAndVersion>> batches,
                                                   Iterator<List<ApiMessageAndVersion>> iterator) throws Exception {
        List<List<ApiMessageAndVersion>> actual = new ArrayList<>();
        while (iterator.hasNext()) {
            actual.add(new ArrayList<>(iterator.next()));
        }
        deepSortRecords(actual);
        List<List<ApiMessageAndVersion>> expected = new ArrayList<>();
        for (List<ApiMessageAndVersion> batch : batches) {
            expected.add(new ArrayList<>(batch));
        }
        deepSortRecords(expected);
        assertEquals(expected, actual);
    }

    @SuppressWarnings("unchecked")
    public static void deepSortRecords(Object o) throws Exception {
        if (o == null) {
            return;
        } else if (o instanceof List) {
            List<?> list = (List<?>) o;
            for (Object entry : list) {
                if (entry != null) {
                    if (Number.class.isAssignableFrom(entry.getClass())) {
                        return;
                    }
                    deepSortRecords(entry);
                }
            }
            list.sort(Comparator.comparing(Object::toString));
        } else if (o instanceof ImplicitLinkedHashCollection) {
            ImplicitLinkedHashCollection<?> coll = (ImplicitLinkedHashCollection<?>) o;
            for (Object entry : coll) {
                deepSortRecords(entry);
            }
            coll.sort(Comparator.comparing(Object::toString));
        } else if (o instanceof Message || o instanceof ApiMessageAndVersion) {
            for (Field field : o.getClass().getDeclaredFields()) {
                field.setAccessible(true);
                deepSortRecords(field.get(o));
            }
        }
    }
}
