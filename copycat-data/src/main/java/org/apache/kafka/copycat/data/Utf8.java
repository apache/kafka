/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/



package org.apache.kafka.copycat.data;

import java.nio.charset.Charset;

/** A Utf8 string.  Unlike {@link String}, instances are mutable.  This is more
 * efficient than {@link String} when reading or writing a sequence of values,
 * as a single instance may be reused. */
public class Utf8 implements Comparable<Utf8>, CharSequence {
    private static final byte[] EMPTY = new byte[0];
    private static final Charset UTF8 = Charset.forName("UTF-8");

    private byte[] bytes = EMPTY;
    private int length;
    private String string;

    public Utf8() {
    }

    public Utf8(String string) {
        this.bytes = getBytesFor(string);
        this.length = bytes.length;
        this.string = string;
    }

    public Utf8(Utf8 other) {
        this.length = other.length;
        this.bytes = new byte[other.length];
        System.arraycopy(other.bytes, 0, this.bytes, 0, this.length);
        this.string = other.string;
    }

    public Utf8(byte[] bytes) {
        this.bytes = bytes;
        this.length = bytes.length;
    }

    /** Return UTF-8 encoded bytes.
     * Only valid through {@link #getByteLength()}. */
    public byte[] getBytes() {
        return bytes;
    }

    /** Return length in bytes.
     * @deprecated call {@link #getByteLength()} instead. */
    public int getLength() {
        return length;
    }

    /** Return length in bytes. */
    public int getByteLength() {
        return length;
    }

    /** Set length in bytes.  Should called whenever byte content changes, even
     * if the length does not change, as this also clears the cached String.
     * @deprecated call {@link #setByteLength(int)} instead. */
    public Utf8 setLength(int newLength) {
        return setByteLength(newLength);
    }

    /** Set length in bytes.  Should called whenever byte content changes, even
     * if the length does not change, as this also clears the cached String. */
    public Utf8 setByteLength(int newLength) {
        if (this.bytes.length < newLength) {
            byte[] newBytes = new byte[newLength];
            System.arraycopy(bytes, 0, newBytes, 0, this.length);
            this.bytes = newBytes;
        }
        this.length = newLength;
        this.string = null;
        return this;
    }

    /** Set to the contents of a String. */
    public Utf8 set(String string) {
        this.bytes = getBytesFor(string);
        this.length = bytes.length;
        this.string = string;
        return this;
    }

    @Override
    public String toString() {
        if (this.length == 0) return "";
        if (this.string == null) {
            this.string = new String(bytes, 0, length, UTF8);
        }
        return this.string;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof Utf8)) return false;
        Utf8 that = (Utf8) o;
        if (!(this.length == that.length)) return false;
        byte[] thatBytes = that.bytes;
        for (int i = 0; i < this.length; i++)
            if (bytes[i] != thatBytes[i])
                return false;
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        for (int i = 0; i < this.length; i++)
            hash = hash * 31 + bytes[i];
        return hash;
    }

    @Override
    public int compareTo(Utf8 that) {
        return BinaryData.compareBytes(this.bytes, 0, this.length,
                that.bytes, 0, that.length);
    }

    // CharSequence implementation
    @Override
    public char charAt(int index) {
        return toString().charAt(index);
    }

    @Override
    public int length() {
        return toString().length();
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return toString().subSequence(start, end);
    }

    /** Gets the UTF-8 bytes for a String */
    public static final byte[] getBytesFor(String str) {
        return str.getBytes(UTF8);
    }

}
