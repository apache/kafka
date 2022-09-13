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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LoggingSignalHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggingSignalHandler.class);

    private static final List<String> SIGNALS = Arrays.asList("TERM", "INT", "HUP");

    private final Constructor<?> signalConstructor;
    private final Class<?> signalHandlerClass;
    private final Class<?> signalClass;
    private final Method signalHandleMethod;
    private final Method signalGetNameMethod;
    private final Method signalHandlerHandleMethod;

    /**
     * Create an instance of this class.
     *
     * @throws ReflectiveOperationException if the underlying API has changed in an incompatible manner.
     */
    public LoggingSignalHandler() throws ReflectiveOperationException {
        signalClass = Class.forName("sun.misc.Signal");
        signalConstructor = signalClass.getConstructor(String.class);
        signalHandlerClass = Class.forName("sun.misc.SignalHandler");
        signalHandlerHandleMethod = signalHandlerClass.getMethod("handle", signalClass);
        signalHandleMethod = signalClass.getMethod("handle", signalClass, signalHandlerClass);
        signalGetNameMethod = signalClass.getMethod("getName");
    }

    /**
     * Register signal handler to log termination due to SIGTERM, SIGHUP and SIGINT (control-c). This method
     * does not currently work on Windows.
     *
     * @implNote sun.misc.Signal and sun.misc.SignalHandler are described as "not encapsulated" in
     * http://openjdk.java.net/jeps/260. However, they are not available in the compile classpath if the `--release`
     * flag is used. As a workaround, we rely on reflection.
     */
    public void register() throws ReflectiveOperationException {
        Map<String, Object> jvmSignalHandlers = new ConcurrentHashMap<>();

        for (String signal : SIGNALS) {
            register(signal, jvmSignalHandlers);
        }
        log.info("Registered signal handlers for " + String.join(", ", SIGNALS));
    }

    private Object createSignalHandler(final Map<String, Object> jvmSignalHandlers) {
        InvocationHandler invocationHandler = new InvocationHandler() {

            private String getName(Object signal) throws Throwable {
                try {
                    return (String) signalGetNameMethod.invoke(signal);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            }

            private void handle(Object signalHandler, Object signal) throws ReflectiveOperationException {
                signalHandlerHandleMethod.invoke(signalHandler, signal);
            }

            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                Object signal = args[0];
                log.info("Terminating process due to signal {}", signal);
                Object handler = jvmSignalHandlers.get(getName(signal));
                if (handler != null)
                    handle(handler, signal);
                return null;
            }
        };
        return Proxy.newProxyInstance(Utils.getContextOrKafkaClassLoader(), new Class[] {signalHandlerClass},
                invocationHandler);
    }

    private void register(String signalName, final Map<String, Object> jvmSignalHandlers) throws ReflectiveOperationException {
        Object signal = signalConstructor.newInstance(signalName);
        Object signalHandler = createSignalHandler(jvmSignalHandlers);
        Object oldHandler = signalHandleMethod.invoke(null, signal, signalHandler);
        if (oldHandler != null)
            jvmSignalHandlers.put(signalName, oldHandler);
    }
}
