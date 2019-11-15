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

package org.apache.kafka.trogdor.coordinator;

import org.glassfish.jersey.internal.inject.AbstractBinder;

/**
 * This class implements a workaround for the Jersey bug which leads to
 * warning messages when registering a Resource object.
 * See https://github.com/eclipse-ee4j/jersey/issues/3700
 *
 * Unfortunately, this can't be common code shared with the Agent, because hk2/jersey
 * doesn't add new bindings for the same class.
 */
public class CoordinatorResourceBinder extends AbstractBinder {
    private Object object;
    private Class clazz;

    public CoordinatorResourceBinder(Object object, Class clazz) {
        this.object = object;
        this.clazz = clazz;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure() {
        bind(object).to(clazz);
    }
}
