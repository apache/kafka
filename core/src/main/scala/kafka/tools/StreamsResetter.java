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
package kafka.tools;


import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@Deprecated
public class StreamsResetter {

    public static void main(String[] args) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        System.out.println("WARNING: The 'kafka.tools' package is deprecated and will change to 'org.apache.kafka.tools' in the next major release.");
        Class<?> toolClass = Class.forName("org.apache.kafka.tools.StreamsResetter");
        Method main = toolClass.getDeclaredMethod("main", String[].class);
        main.invoke(null, (Object) args);
    }
}
