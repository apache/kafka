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
package org.apache.kafka.connect.runtime.isolation;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;

abstract class AbstractModuleFactory<T> {
    protected final DelegatingClassLoader loader;

    public AbstractModuleFactory(DelegatingClassLoader loader) {
        this.loader = loader;
    }

    public DelegatingClassLoader getLoader() {
        return loader;
    }

    public T newModule(String classOrAlias, Class<T> klass) {
         try {
            return instantiate(moduleClass(loader, classOrAlias, klass));
        } catch (Throwable t) {
            throw new ConnectException("Instantiation error", t);
        }
    }

    protected static <R> R instantiate(Class<? extends R> cls) {
        try {
            return Utils.newInstance(cls);
        } catch (Throwable t) {
            throw new ConnectException("Instantiation error", t);
        }
    }

    @SuppressWarnings("unchecked")
    protected Class<? extends T> moduleClass(
            DelegatingClassLoader loader,
            String classOrAlias,
            Class<T> moduleClass
    ) throws ClassNotFoundException {
        Class<?> klass = loader.loadClass(classOrAlias, false);
        if (moduleClass.isAssignableFrom(klass)) {
            return (Class<? extends T>) klass;
        }

        throw new ClassNotFoundException(
                "Requested class: "
                + classOrAlias
                + " does not extend Connector interface"
        );
    }
}
