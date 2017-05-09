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

import java.net.URL;
import java.net.URLClassLoader;

public class ModuleClassLoader extends URLClassLoader {
    private static final String[] EXCLUSIONS = {"java.", "javax.", "org.apache.kafka."};

    public ModuleClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    public ModuleClassLoader(URL[] urls) {
        super(urls);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        Class<?> klass = findLoadedClass(name);
        if (klass == null) {
            if (validate(name)) {
                try {
                    klass = findClass(name);
                } catch (ClassNotFoundException e) {
                    // Not found in loader's path. Search in parents.
                }
            }
            if (klass == null) {
                klass = super.loadClass(name, false);
            }
        }
        if (resolve) {
            resolveClass(klass);
        }
        return klass;
    }

    protected boolean validate(String name) {
        boolean result = false;
        for (String exclusion : EXCLUSIONS) {
            result |= name.startsWith(exclusion);
        }
        return !result;
    }
}
