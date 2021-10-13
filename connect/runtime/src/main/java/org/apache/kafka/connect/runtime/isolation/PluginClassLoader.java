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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * A custom classloader dedicated to loading Connect plugin classes in classloading isolation.
 * <p>
 * Under the current scheme for classloading isolation in Connect, a plugin classloader loads the
 * classes that it finds in its urls. For classes that are either not found or are not supposed to
 * be loaded in isolation, this plugin classloader delegates their loading to its parent. This makes
 * this classloader a child-first classloader.
 * <p>
 * This class is thread-safe and parallel capable.
 */
public class PluginClassLoader extends URLClassLoader {
    private static final Logger log = LoggerFactory.getLogger(PluginClassLoader.class);
    private final URL pluginLocation;

    static {
        ClassLoader.registerAsParallelCapable();
    }

    /**
     * Constructor that accepts a specific classloader as parent.
     *
     * @param pluginLocation the top-level location of the plugin to be loaded in isolation by this
     * classloader.
     * @param urls the list of urls from which to load classes and resources for this plugin.
     * @param parent the parent classloader to be used for delegation for classes that were
     * not found or should not be loaded in isolation by this classloader.
     */
    public PluginClassLoader(URL pluginLocation, URL[] urls, ClassLoader parent) {
        super(urls, parent);
        this.pluginLocation = pluginLocation;
    }

    /**
     * Constructor that defines the system classloader as parent of this plugin classloader.
     *
     * @param pluginLocation the top-level location of the plugin to be loaded in isolation by this
     * classloader.
     * @param urls the list of urls from which to load classes and resources for this plugin.
     */
    public PluginClassLoader(URL pluginLocation, URL[] urls) {
        super(urls);
        this.pluginLocation = pluginLocation;
    }

    /**
     * Returns the top-level location of the classes and dependencies required by the plugin that
     * is loaded by this classloader.
     *
     * @return the plugin location.
     */
    public String location() {
        return pluginLocation.toString();
    }

    @Override
    public String toString() {
        return "PluginClassLoader{pluginLocation=" + pluginLocation + "}";
    }

    // This method needs to be thread-safe because it is supposed to be called by multiple
    // Connect tasks. While findClass is thread-safe, defineClass called within loadClass of the
    // base method is not. More on multithreaded classloaders in:
    // https://docs.oracle.com/javase/7/docs/technotes/guides/lang/cl-mt.html
    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> klass = findLoadedClass(name);
            if (klass == null) {
                try {
                    if (PluginUtils.shouldLoadInIsolation(name)) {
                        klass = findClass(name);
                    }
                } catch (ClassNotFoundException e) {
                    // Not found in loader's path. Search in parents.
                    log.trace("Class '{}' not found. Delegating to parent", name);
                }
            }
            if (klass == null) {
                klass = super.loadClass(name, false);
            }
            if (resolve) {
                resolveClass(klass);
            }
            return klass;
        }
    }
}
