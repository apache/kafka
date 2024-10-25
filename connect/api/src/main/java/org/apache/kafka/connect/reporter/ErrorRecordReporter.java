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
package org.apache.kafka.connect.reporter;

import org.apache.kafka.common.Configurable;

/**
 * Provides a mechanism for reporting errors
 * using the information contained in an `ErrorContext`.
 * <p>
 * Implementations of this interface are responsible for reporting errors
 * that occur during the execution of a Kafka Connect task.
 *
 * @param <T> the type of the error context
 */
public interface ErrorRecordReporter<T> extends Configurable, AutoCloseable {

	/**
	 * Report an error using the provided error context.
	 *
	 * @param context the error context (cannot be null)
	 */
	void report(ErrorContext<T> context);

	@Override
	default void close() {
	}
}
