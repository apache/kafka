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
package org.apache.kafka.connect.runtime.errors.impl;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.runtime.errors.ProcessingContext;
import org.apache.kafka.connect.runtime.errors.Stage;
import org.apache.kafka.connect.runtime.errors.StageType;
import org.apache.kafka.connect.runtime.errors.Structable;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class StructUtil {

    private static final Logger log = LoggerFactory.getLogger(StructUtil.class);

    public static Struct toStruct(Structable structable) {
        Objects.requireNonNull(structable);

        Schema objectSchema = getSchemaFor(structable.getClass());
        return getStructFor(structable, objectSchema);
    }

    @SuppressWarnings("unchecked")
    private static Struct getStructFor(Structable structable, Schema schema) {
        Objects.requireNonNull(structable);

        Struct struct = new Struct(schema);
        Method[] methods = structable.getClass().getMethods();
        for (Method method: methods) {
            Structable.Field field = method.getAnnotation(Structable.Field.class);
            if (field == null) {
                continue;
            }
            if (method.getParameterTypes().length > 0) {
                log.debug("Cannot invoke method with non-zero parameters.");
                continue;
            }
            try {
                Object object = method.invoke(structable);
                if (object != null) {
                    if (object instanceof Exception) {
                        Exception ex = (Exception) object;
                        object = getMessage(ex) + "\n" + getStackTrace(ex);
                    } else if (object.getClass().isEnum()) {
                        object = String.valueOf(object);
                    } else if (Class.class.isAssignableFrom(object.getClass())) {
                        object = ((Class<?>) object).getName();
                    } else if (object instanceof Map) {
                        Map<String, String> m = new HashMap<>();
                        Map<Object, Object> mm = (Map<Object, Object>) object;
                        for (Map.Entry<Object, Object> e: mm.entrySet()) {
                            String key = e.getKey() instanceof String ? (String) e.getKey() : String.valueOf(e.getKey());
                            String val = e.getValue() instanceof String ? (String) e.getValue() : String.valueOf(e.getValue());
                            m.put(key, val);
                        }
                        object = m;
                    }
                    struct.put(field.value(), object);
                }

            } catch (Exception e) {
                log.error("Could nto invoke method " + method, e);
            }
        }
        return struct;
    }

    @SuppressWarnings("unchecked")
    public static Schema getSchemaFor(Class<? extends Structable> structable) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        Method[] methods = structable.getMethods();
        for (Method method: methods) {
            Structable.Field field = method.getAnnotation(Structable.Field.class);
            if (field == null) {
                continue;
            }
            if (method.getParameterTypes().length > 0) {
                log.debug("Cannot invoke method with non-zero parameters.");
                continue;
            }

            Class<?> type = method.getReturnType();
            if (method.getReturnType().equals(int.class)) {
                schemaBuilder.field(field.value(), Schema.OPTIONAL_INT32_SCHEMA);
            } else if (method.getReturnType().equals(String.class)) {
                schemaBuilder.field(field.value(), Schema.OPTIONAL_STRING_SCHEMA);
            } else if (method.getReturnType().equals(long.class)) {
                schemaBuilder.field(field.value(), Schema.OPTIONAL_INT64_SCHEMA);
            } else if (method.getReturnType().equals(Exception.class)) {
                schemaBuilder.field(field.value(), Schema.OPTIONAL_STRING_SCHEMA);
            } else if (method.getReturnType().equals(Structable.class)) {
                schemaBuilder.field(field.value(), getSchemaFor((Class<? extends Structable>) method.getReturnType()));
            } else if (method.getReturnType().isEnum()) {
                schemaBuilder.field(field.value(), Schema.OPTIONAL_STRING_SCHEMA);
            } else if (Class.class.isAssignableFrom(method.getReturnType())) {
                schemaBuilder.field(field.value(), Schema.OPTIONAL_STRING_SCHEMA);
            } else if (Map.class.isAssignableFrom(method.getReturnType())) {
                schemaBuilder.field(field.value(), SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).build());
            } else {
                throw new DataException("Could not translate type: " + type + " for method " + method);
            }
        }
        return schemaBuilder.build();
    }

    public static String getMessage(final Throwable th) {
        if (th == null) {
            return "";
        }
        final String clsName = th.getClass().getName();
        final String msg = th.getMessage();
        return clsName + ": " + msg;
    }

    public static String getStackTrace(final Throwable throwable) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw, true);
        throwable.printStackTrace(pw);
        return sw.getBuffer().toString();
    }

    public static void main(String[] args) {
        new StructUtil().test();
    }

    private Exception foo() {
        return new ConnectException("connect");
    }

    private void test() {
        System.out.println(RuntimeException.class.isAssignableFrom(Exception.class));
        System.out.println(Exception.class.isAssignableFrom(RuntimeException.class));
        System.out.println(RuntimeException.class.isAssignableFrom(RuntimeException.class));

        ProcessingContext context = ProcessingContext.noop("my-task");
        context.setException(new RuntimeException("hello", foo()));
        Struct contextStruct = toStruct(context);

        Map<String, Object> config = new HashMap<>();
        config.put("k1", "v1");
        config.put("k2", 10);
        Stage stage = Stage.newBuilder(StageType.TASK_START).setExecutingClass(SinkTask.class).setConfig(config).build();
        Struct stageStruct = toStruct(stage);

        Schema combinedSchema = SchemaBuilder.struct()
                .field("context", contextStruct.schema())
                .field("stage", stageStruct.schema())
                .build();
        Struct struct = new Struct(combinedSchema);
        struct.put("context", contextStruct);
        struct.put("stage", stageStruct);

        System.out.println(struct.toString().replaceAll("\n", "\\\\n"));
    }
}
