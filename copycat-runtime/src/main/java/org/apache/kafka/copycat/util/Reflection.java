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

package org.apache.kafka.copycat.util;

import org.apache.kafka.copycat.errors.CopycatRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

public class Reflection {
    private static final Logger log = LoggerFactory.getLogger(Reflection.class);

    /**
     * Instantiate the class, which is a subclass of the specified type.
     * @param className fully qualified name of the class to instantiate
     * @param superType required supertype of the class
     * @param <T>
     * @return a new instance of the class instantiated with the default constructor
     */
    public static <T> T instantiate(String className, Class<T> superType) {
        try {
            Class<? extends T> taskClass = Class.forName(className).asSubclass(superType);
            return taskClass.getConstructor().newInstance();
        } catch (ClassNotFoundException e) {
            log.error("Class not found: " + className, e);
            throw new CopycatRuntimeException(e);
        } catch (ClassCastException e) {
            log.error("Specified class " + className + " is not a subclass of " + superType + ":", e);
            throw new CopycatRuntimeException(e);
        } catch (NoSuchMethodException e) {
            log.error("Class does not have a default constructor: " + className, e);
            throw new CopycatRuntimeException(e);
        } catch (InstantiationException e) {
            log.error("Class couldn't be instantiated: " + className, e);
            throw new CopycatRuntimeException(e);
        } catch (InvocationTargetException e) {
            log.error("Class constructor threw an exception: " + className, e);
            throw new CopycatRuntimeException(e);
        } catch (IllegalAccessException e) {
            log.error("Class couldn't be instantiated due to IllegalAccessException: " + className, e);
            throw new CopycatRuntimeException(e);
        }
    }

    /**
     * Instantiate the class, which is a subclass of the specified type, and start it.
     * @param className fully qualified name of the class to instantiate
     * @param superType required supertype of the class
     * @param <T>
     * @return a new instance of the class instantiated with the default constructor
     */
    public static <T extends Configurable> T instantiateConfigurable(
            String className, Class<T> superType, Properties props) {
        T result = instantiate(className, superType);
        result.configure(props);
        return result;
    }
}
