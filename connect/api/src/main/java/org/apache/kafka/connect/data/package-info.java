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
/**
 * Provides classes for representing data and schemas handled by Connect.
 *
 * <p>In addition to basic data types like integers, strings, and maps, Connect also supports logical types
 * that add additional semantic meaning to the basic types:</p>
 * <ul>
 *   <li>{@link org.apache.kafka.connect.data.Decimal} - Arbitrary precision decimal numbers</li>
 *   <li>{@link org.apache.kafka.connect.data.Date} - Calendar date (year, month, day) without time or timezone</li>
 *   <li>{@link org.apache.kafka.connect.data.Time} - Time of day (hours, minutes, seconds, milliseconds) without date or timezone</li>
 *   <li>{@link org.apache.kafka.connect.data.Timestamp} - Absolute point in time with millisecond precision</li>
 *   <li>{@link org.apache.kafka.connect.data.TimestampMicros} - Absolute point in time with microsecond precision</li>
 * </ul>
 */
package org.apache.kafka.connect.data;