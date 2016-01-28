/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.kafka.common.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.File;
import java.io.FileInputStream;

public class Serializer {

    public static byte[] serialize(Object toSerialize) throws IOException {
        byte[] byteArray = null;
        ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream ooStream = new ObjectOutputStream(arrayOutputStream);

        ooStream.writeObject(toSerialize);
        byteArray = arrayOutputStream.toByteArray();

        ooStream.close();
        arrayOutputStream.close();
        return byteArray;
    }

    public static Object deserialize(InputStream inputStream) throws IOException, ClassNotFoundException {
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        Object deserializedObject = objectInputStream.readObject();
        objectInputStream.close();
        inputStream.close();
        return deserializedObject;
    }

    public static Object deserialize(byte[] byteArray) throws IOException, ClassNotFoundException {
        ByteArrayInputStream arrayInputStream = new ByteArrayInputStream(byteArray);
        return deserialize(arrayInputStream);
    }

    public static Object deserialize(String fileName) throws IOException, ClassNotFoundException {
        ClassLoader classLoader = Serializer.class.getClassLoader();
        File file = new File(classLoader.getResource(fileName).getFile());
        FileInputStream fis = new FileInputStream(file);

        return deserialize(fis);
    }
}
