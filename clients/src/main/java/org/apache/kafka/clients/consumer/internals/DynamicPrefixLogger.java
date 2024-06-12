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
package org.apache.kafka.clients.consumer.internals;

import org.slf4j.Logger;
import org.slf4j.Marker;

import java.util.function.Supplier;

public final class DynamicPrefixLogger implements Logger {

    private final Supplier<String> prefix;
    private final Logger delegate;

    public DynamicPrefixLogger(final Supplier<String> prefix, final Logger delegate) {
        this.prefix = prefix;
        this.delegate = delegate;
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public boolean isTraceEnabled() {
        return delegate.isTraceEnabled();
    }

    @Override
    public void trace(final String msg) {
        delegate.trace(prefix.get() + msg);
    }

    @Override
    public void trace(final String format, final Object arg) {
        delegate.trace(prefix.get() + format, arg);
    }

    @Override
    public void trace(final String format, final Object arg1, final Object arg2) {
        delegate.trace(prefix.get() + format, arg1, arg2);
    }

    @Override
    public void trace(final String format, final Object... arguments) {
        delegate.trace(prefix.get() + format, arguments);
    }

    @Override
    public void trace(final String msg, final Throwable t) {
        delegate.trace(prefix.get() + msg, t);
    }

    @Override
    public boolean isTraceEnabled(final Marker marker) {
        return delegate.isTraceEnabled(marker);
    }

    @Override
    public void trace(final Marker marker, final String msg) {
        delegate.trace(marker, prefix.get() + msg);
    }

    @Override
    public void trace(final Marker marker, final String format, final Object arg) {
        delegate.trace(marker, prefix.get() + format, arg);
    }

    @Override
    public void trace(final Marker marker,
                      final String format,
                      final Object arg1,
                      final Object arg2) {
        delegate.trace(marker, prefix.get() + format, arg1, arg2);
    }

    @Override
    public void trace(final Marker marker, final String format, final Object... argArray) {
        delegate.trace(marker, prefix.get() + format, argArray);
    }

    @Override
    public void trace(final Marker marker, final String msg, final Throwable t) {
        delegate.trace(marker, prefix.get() + msg, t);
    }

    @Override
    public boolean isDebugEnabled() {
        return delegate.isDebugEnabled();
    }

    @Override
    public void debug(final String msg) {
        delegate.debug(prefix.get() + msg);
    }

    @Override
    public void debug(final String format, final Object arg) {
        delegate.debug(prefix.get() + format, arg);
    }

    @Override
    public void debug(final String format, final Object arg1, final Object arg2) {
        delegate.debug(prefix.get() + format, arg1, arg2);
    }

    @Override
    public void debug(final String format, final Object... arguments) {
        delegate.debug(prefix.get() + format, arguments);
    }

    @Override
    public void debug(final String msg, final Throwable t) {
        delegate.debug(prefix.get() + msg, t);
    }

    @Override
    public boolean isDebugEnabled(final Marker marker) {
        return delegate.isDebugEnabled(marker);
    }

    @Override
    public void debug(final Marker marker, final String msg) {
        delegate.debug(marker, prefix.get() + msg);
    }

    @Override
    public void debug(final Marker marker, final String format, final Object arg) {
        delegate.debug(marker, prefix.get() + format, arg);
    }

    @Override
    public void debug(final Marker marker,
                      final String format,
                      final Object arg1,
                      final Object arg2) {
        delegate.debug(marker, prefix.get() + format, arg1, arg2);
    }

    @Override
    public void debug(final Marker marker, final String format, final Object... arguments) {
        delegate.debug(marker, prefix.get() + format, arguments);
    }

    @Override
    public void debug(final Marker marker, final String msg, final Throwable t) {
        delegate.debug(marker, prefix.get() + msg, t);
    }

    @Override
    public boolean isInfoEnabled() {
        return delegate.isInfoEnabled();
    }

    @Override
    public void info(final String msg) {
        delegate.info(prefix.get() + msg);
    }

    @Override
    public void info(final String format, final Object arg) {
        delegate.info(prefix.get() + format, arg);
    }

    @Override
    public void info(final String format, final Object arg1, final Object arg2) {
        delegate.info(prefix.get() + format, arg1, arg2);
    }

    @Override
    public void info(final String format, final Object... arguments) {
        delegate.info(prefix.get() + format, arguments);
    }

    @Override
    public void info(final String msg, final Throwable t) {
        delegate.info(prefix.get() + msg, t);
    }

    @Override
    public boolean isInfoEnabled(final Marker marker) {
        return delegate.isInfoEnabled(marker);
    }

    @Override
    public void info(final Marker marker, final String msg) {
        delegate.info(marker, prefix.get() + msg);
    }

    @Override
    public void info(final Marker marker, final String format, final Object arg) {
        delegate.info(marker, prefix.get() + format, arg);
    }

    @Override
    public void info(final Marker marker,
                     final String format,
                     final Object arg1,
                     final Object arg2) {
        delegate.info(marker, prefix.get() + format, arg1, arg2);
    }

    @Override
    public void info(final Marker marker, final String format, final Object... arguments) {
        delegate.info(marker, prefix.get() + format, arguments);
    }

    @Override
    public void info(final Marker marker, final String msg, final Throwable t) {
        delegate.info(marker, prefix.get() + msg, t);
    }

    @Override
    public boolean isWarnEnabled() {
        return delegate.isWarnEnabled();
    }

    @Override
    public void warn(final String msg) {
        delegate.warn(prefix.get() + msg);
    }

    @Override
    public void warn(final String format, final Object arg) {
        delegate.warn(prefix.get() + format, arg);
    }

    @Override
    public void warn(final String format, final Object... arguments) {
        delegate.warn(prefix.get() + format, arguments);
    }

    @Override
    public void warn(final String format, final Object arg1, final Object arg2) {
        delegate.warn(prefix.get() + format, arg1, arg2);
    }

    @Override
    public void warn(final String msg, final Throwable t) {
        delegate.warn(prefix.get() + msg, t);
    }

    @Override
    public boolean isWarnEnabled(final Marker marker) {
        return delegate.isWarnEnabled(marker);
    }

    @Override
    public void warn(final Marker marker, final String msg) {
        delegate.warn(marker, prefix.get() + msg);
    }

    @Override
    public void warn(final Marker marker, final String format, final Object arg) {
        delegate.warn(marker, prefix.get() + format, arg);
    }

    @Override
    public void warn(final Marker marker,
                     final String format,
                     final Object arg1,
                     final Object arg2) {
        delegate.warn(marker, prefix.get() + format, arg1, arg2);
    }

    @Override
    public void warn(final Marker marker, final String format, final Object... arguments) {
        delegate.warn(marker, prefix.get() + format, arguments);
    }

    @Override
    public void warn(final Marker marker, final String msg, final Throwable t) {
        delegate.warn(marker, prefix.get() + msg, t);
    }

    @Override
    public boolean isErrorEnabled() {
        return delegate.isErrorEnabled();
    }

    @Override
    public void error(final String msg) {
        delegate.error(prefix.get() + msg);
    }

    @Override
    public void error(final String format, final Object arg) {
        delegate.error(prefix.get() + format, arg);
    }

    @Override
    public void error(final String format, final Object arg1, final Object arg2) {
        delegate.error(prefix.get() + format, arg1, arg2);
    }

    @Override
    public void error(final String format, final Object... arguments) {
        delegate.error(prefix.get() + format, arguments);
    }

    @Override
    public void error(final String msg, final Throwable t) {
        delegate.error(prefix.get() + msg, t);
    }

    @Override
    public boolean isErrorEnabled(final Marker marker) {
        return delegate.isErrorEnabled(marker);
    }

    @Override
    public void error(final Marker marker, final String msg) {
        delegate.error(marker, prefix.get() + msg);
    }

    @Override
    public void error(final Marker marker, final String format, final Object arg) {
        delegate.error(marker, format, arg);
    }

    @Override
    public void error(final Marker marker,
                      final String format,
                      final Object arg1,
                      final Object arg2) {
        delegate.error(marker, prefix.get() + format, arg1, arg2);
    }

    @Override
    public void error(final Marker marker, final String format, final Object... arguments) {
        delegate.error(marker, prefix.get() + format, arguments);
    }

    @Override
    public void error(final Marker marker, final String msg, final Throwable t) {
        delegate.error(marker, prefix.get() + msg, t);
    }
}
