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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.util.Closeables;

import java.util.HashMap;
import java.util.Map;

public class TestCloseables extends Closeables {

    private final Map<Class<? extends AutoCloseable>, Integer> registered;

    private ClassLoader loader;
    private boolean cleared;

    public TestCloseables() {
        super();
        this.registered = new HashMap<>();
    }

    @Override
    public void register(AutoCloseable closeable, String description) {
        super.register(closeable, description);
        this.registered.compute(closeable.getClass(), (c, numRegistered) -> numRegistered == null ? 1 : numRegistered + 1);
    }

    @Override
    public void useLoader(ClassLoader loader) {
        super.useLoader(loader);
        this.loader = loader;
    }

    @Override
    public void clear() {
        super.clear();
        this.cleared = true;
    }

    public Map<Class<? extends AutoCloseable>, Integer> registered() {
        return registered;
    }

    public boolean cleared() {
        return cleared;
    }

    public ClassLoader loader() {
        return loader;
    }

}
