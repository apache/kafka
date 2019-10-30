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
package org.apache.kafka.common.utils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.NoSuchElementException;

/**
 * A class loader that looks for classes and resources in a specified class path first, before delegating to its parent
 * class loader.
 */
public class ChildFirstClassLoader extends URLClassLoader {
    static {
        ClassLoader.registerAsParallelCapable();
    }

    /**
     * @param classPath Class path string
     * @param parent    The parent classloader. If the required class / resource cannot be found in the given classPath,
     *                  this classloader will be used to find the class / resource.
     */
    public ChildFirstClassLoader(String classPath, ClassLoader parent) {
        super(classpath2URLs(classPath), parent);
    }

    static private URL[] classpath2URLs(String classPath) {
        ArrayList<URL> urls = new ArrayList<>();
        for (String path : classPath.split(File.pathSeparator)) {
            if (path == null || path.trim().isEmpty())
                continue;
            File f = new File(path);

            if (path.endsWith("/*")) {
                try {
                    File parent = new File(new File(f.getCanonicalPath()).getParent());
                    if (parent.isDirectory()) {
                        File[] files = parent.listFiles((dir, name) -> {
                            String lower = name.toLowerCase();
                            return lower.endsWith(".jar") || lower.endsWith(".zip");
                        });
                        for (File jarFile : files) {
                            urls.add(jarFile.getCanonicalFile().toURI().toURL());
                        }
                    }
                } catch (IOException e) {
                }
            } else if (f.exists()) {
                try {
                    urls.add(f.getCanonicalFile().toURI().toURL());
                } catch (IOException e) {
                }
            }
        }
        return urls.toArray(new URL[0]);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> c = findLoadedClass(name);

            if (c == null) {
                try {
                    c = findClass(name);
                } catch (ClassNotFoundException e) {
                    // try parent
                    c = super.loadClass(name, false);
                }
            }

            if (resolve)
                resolveClass(c);

            return c;
        }
    }

    @Override
    public URL getResource(String name) {
        URL url = findResource(name);
        if (url == null) {
            // try parent
            url = super.getResource(name);
        }
        return url;
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        Enumeration<URL> urls1 = findResources(name);
        Enumeration<URL> urls2 = getParent() != null ? getParent().getResources(name) : null;

        return new Enumeration<URL>() {
            @Override
            public boolean hasMoreElements() {
                return (urls1 != null && urls1.hasMoreElements()) || (urls2 !=null && urls2.hasMoreElements());
            }

            @Override
            public URL nextElement() {
                if (urls1 != null && urls1.hasMoreElements())
                    return urls1.nextElement();
                if (urls2 != null && urls2.hasMoreElements())
                    return urls2.nextElement();
                throw new NoSuchElementException();
            }
        };
    }
}
