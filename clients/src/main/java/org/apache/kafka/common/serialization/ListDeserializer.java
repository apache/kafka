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
package org.apache.kafka.common.serialization;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

public class ListDeserializer<T> implements Deserializer<List<T>> {

	private final Deserializer<T> deserializer;
	private final Comparator<T> comparator;

	public ListDeserializer(Deserializer<T> deserializer, Comparator<T> comparator) {
		this.deserializer = deserializer;
		this.comparator = comparator;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// Do nothing
	}

	@Override
	public List<T> deserialize(String topic, byte[] data) {
		if (data == null || data.length == 0) {
			return null;
		}
		@SuppressWarnings("serial")
		List<T> deserializedList = new ArrayList<T>() {
			@Override
			public void sort(Comparator<? super T> c) {
				super.sort(comparator);
			}
		};
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
		try {
			final int size = dis.readInt();
			for (int i = 0; i < size; i++) {
				byte[] payload = new byte[dis.readInt()];
				dis.read(payload);
				deserializedList.add(deserializer.deserialize(topic, payload));
			}
			dis.close();
		} catch (IOException e) {
			throw new RuntimeException("Unable to deserialize into a List", e);
		}
		return deserializedList;
	}

	@Override
	public void close() {
		// Do nothing
	}

}