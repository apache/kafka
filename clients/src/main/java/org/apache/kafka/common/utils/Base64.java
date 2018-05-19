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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.regex.Pattern;

/**
 * Temporary class in order to support Java 7 and Java 9. `DatatypeConverter` is not in the base module of Java 9
 * and `java.util.Base64` was only introduced in Java 8.
 */
public final class Base64 {

    private static final Factory FACTORY;

    static {
        if (Java.IS_JAVA8_COMPATIBLE)
            FACTORY = new Java8Factory();
        else
            FACTORY = new Java7Factory();
    }

    private Base64() {}

    public static Encoder encoder() {
        return FACTORY.encoder();
    }

    public static Encoder urlEncoderNoPadding() {
        return FACTORY.urlEncoderNoPadding();
    }

    public static Decoder decoder() {
        return FACTORY.decoder();
    }

    public static Decoder urlDecoder() {
        return FACTORY.urlDecoder();
    }

    /* Contains a subset of methods from java.util.Base64.Encoder (introduced in Java 8) */
    public interface Encoder {
        String encodeToString(byte[] bytes);
    }

    /* Contains a subset of methods from java.util.Base64.Decoder (introduced in Java 8) */
    public interface Decoder {
        byte[] decode(String string);
    }

    private interface Factory {
        Encoder urlEncoderNoPadding();
        Encoder encoder();
        Decoder urlDecoder();
        Decoder decoder();
    }

    private static class Java8Factory implements Factory {

        // Static final MethodHandles are optimised better by HotSpot
        private static final MethodHandle URL_ENCODE_NO_PADDING;
        private static final MethodHandle ENCODE;
        private static final MethodHandle URL_DECODE;
        private static final MethodHandle DECODE;

        private static final Encoder URL_ENCODER_NO_PADDING;
        private static final Encoder ENCODER;
        private static final Decoder URL_DECODER;
        private static final Decoder DECODER;

        static {
            try {
                Class<?> base64Class = Class.forName("java.util.Base64");

                MethodHandles.Lookup lookup = MethodHandles.publicLookup();

                Class<?> juEncoderClass = Class.forName("java.util.Base64$Encoder");

                MethodHandle getEncoder = lookup.findStatic(base64Class, "getEncoder",
                        MethodType.methodType(juEncoderClass));
                Object juEncoder;
                try {
                    juEncoder = getEncoder.invoke();
                } catch (Throwable throwable) {
                    // Invoked method doesn't throw checked exceptions, so safe to cast
                    throw (RuntimeException) throwable;
                }
                MethodHandle encode = lookup.findVirtual(juEncoderClass, "encodeToString",
                        MethodType.methodType(String.class, byte[].class));
                ENCODE = encode.bindTo(juEncoder);


                MethodHandle getUrlEncoder = lookup.findStatic(base64Class, "getUrlEncoder",
                        MethodType.methodType(juEncoderClass));
                Object juUrlEncoderNoPassing;
                try {
                    juUrlEncoderNoPassing = lookup.findVirtual(juEncoderClass, "withoutPadding",
                            MethodType.methodType(juEncoderClass)).invoke(getUrlEncoder.invoke());
                } catch (Throwable throwable) {
                    // Invoked method doesn't throw checked exceptions, so safe to cast
                    throw (RuntimeException) throwable;
                }
                URL_ENCODE_NO_PADDING = encode.bindTo(juUrlEncoderNoPassing);

                Class<?> juDecoderClass = Class.forName("java.util.Base64$Decoder");
                MethodHandle getDecoder = lookup.findStatic(base64Class, "getDecoder",
                        MethodType.methodType(juDecoderClass));
                MethodHandle decode = lookup.findVirtual(juDecoderClass, "decode",
                        MethodType.methodType(byte[].class, String.class));
                try {
                    DECODE = decode.bindTo(getDecoder.invoke());
                } catch (Throwable throwable) {
                    // Invoked method doesn't throw checked exceptions, so safe to cast
                    throw (RuntimeException) throwable;
                }

                MethodHandle getUrlDecoder = lookup.findStatic(base64Class, "getUrlDecoder",
                        MethodType.methodType(juDecoderClass));
                MethodHandle urlDecode = lookup.findVirtual(juDecoderClass, "decode",
                        MethodType.methodType(byte[].class, String.class));
                try {
                    URL_DECODE = urlDecode.bindTo(getUrlDecoder.invoke());
                } catch (Throwable throwable) {
                    // Invoked method doesn't throw checked exceptions, so safe to cast
                    throw (RuntimeException) throwable;
                }
                
                URL_ENCODER_NO_PADDING = new Encoder() {
                    @Override
                    public String encodeToString(byte[] bytes) {
                        try {
                            return (String) URL_ENCODE_NO_PADDING.invokeExact(bytes);
                        } catch (Throwable throwable) {
                            // Invoked method doesn't throw checked exceptions, so safe to cast
                            throw (RuntimeException) throwable;
                        }
                    }
                };

                ENCODER = new Encoder() {
                    @Override
                    public String encodeToString(byte[] bytes) {
                        try {
                            return (String) ENCODE.invokeExact(bytes);
                        } catch (Throwable throwable) {
                            // Invoked method doesn't throw checked exceptions, so safe to cast
                            throw (RuntimeException) throwable;
                        }
                    }
                };

                URL_DECODER = new Decoder() {
                    @Override
                    public byte[] decode(String string) {
                        try {
                            return (byte[]) URL_DECODE.invokeExact(string);
                        } catch (Throwable throwable) {
                            // Invoked method doesn't throw checked exceptions, so safe to cast
                            throw (RuntimeException) throwable;
                        }
                    }
                };

                DECODER = new Decoder() {
                    @Override
                    public byte[] decode(String string) {
                        try {
                            return (byte[]) DECODE.invokeExact(string);
                        } catch (Throwable throwable) {
                            // Invoked method doesn't throw checked exceptions, so safe to cast
                            throw (RuntimeException) throwable;
                        }
                    }
                };

            } catch (ReflectiveOperationException e) {
                // Should never happen
                throw new RuntimeException(e);
            }
        }

        @Override
        public Encoder urlEncoderNoPadding() {
            return URL_ENCODER_NO_PADDING;
        }

        @Override
        public Encoder encoder() {
            return ENCODER;
        }

        @Override
        public Decoder decoder() {
            return DECODER;
        }

        @Override
        public Decoder urlDecoder() {
            return URL_DECODER;
        }
    }

    private static class Java7Factory implements Factory {

        // Static final MethodHandles are optimised better by HotSpot
        private static final MethodHandle PRINT;
        private static final MethodHandle PARSE;

        static {
            try {
                Class<?> cls = Class.forName("javax.xml.bind.DatatypeConverter");
                MethodHandles.Lookup lookup = MethodHandles.publicLookup();
                PRINT = lookup.findStatic(cls, "printBase64Binary", MethodType.methodType(String.class,
                        byte[].class));
                PARSE = lookup.findStatic(cls, "parseBase64Binary", MethodType.methodType(byte[].class,
                        String.class));
            } catch (ReflectiveOperationException e) {
                // Should never happen
                throw new RuntimeException(e);
            }
        }

        /** Statically compiled Pattern for finding the literal "+" character. */
        private static final Pattern REPLACE_PLUS = Pattern.compile("+", Pattern.LITERAL);
        /** Statically compiled Pattern for finding the literal "/" character. */
        private static final Pattern REPLACE_FORWARD_SLASH = Pattern.compile("/", Pattern.LITERAL);

        public static final Encoder URL_ENCODER_NO_PADDING = new Encoder() {

            @Override
            public String encodeToString(byte[] bytes) {
                if (bytes.length == 0)
                    return "";
                String base64EncodedUUID = Java7Factory.encodeToString(bytes);

                // Convert to URL safe variant by replacing + and / with - and _ respectively.
                String urlSafeBase64EncodedUUID =
                        REPLACE_FORWARD_SLASH.matcher(
                            REPLACE_PLUS.matcher(
                                    base64EncodedUUID
                            ).replaceAll("-")
                        ).replaceAll("_");

                // Remove any "=" or "==" padding at the end.
                // Note that length will be at least 4 here.
                int index = urlSafeBase64EncodedUUID.indexOf('=', urlSafeBase64EncodedUUID.length() - 2);
                return index > 0 ? urlSafeBase64EncodedUUID.substring(0, index) : urlSafeBase64EncodedUUID;
            }

        };

        public static final Encoder ENCODER = new Encoder() {
            @Override
            public String encodeToString(byte[] bytes) {
                return Java7Factory.encodeToString(bytes);
            }
        };

        public static final Decoder DECODER = new Decoder() {
            @Override
            public byte[] decode(String string) {
                try {
                    return (byte[]) PARSE.invokeExact(string);
                } catch (Throwable throwable) {
                    // Invoked method doesn't throw checked exceptions, so safe to cast
                    throw (RuntimeException) throwable;
                }
            }
        };

        /** Statically compiled Pattern for finding the literal "+" character. */
        private static final Pattern REPLACE_MINUS = Pattern.compile("-", Pattern.LITERAL);
        /** Statically compiled Pattern for finding the literal "/" character. */
        private static final Pattern REPLACE_UNDERSCORE = Pattern.compile("_", Pattern.LITERAL);

        public static final Decoder URL_DECODER = new Decoder() {
            @Override
            public byte[] decode(String string) {
                try {
                    // Convert from URL safe variant by replacing - and _ with + and / respectively,
                    // and append "=" or "==" padding; then decode.
                    String unpadded =
                            REPLACE_UNDERSCORE.matcher(
                                    REPLACE_MINUS.matcher(
                                            string
                                    ).replaceAll("+")
                            ).replaceAll("/");

                    int padLength = 4 - (unpadded.length() & 3);
                    return (byte[]) PARSE.invokeExact(padLength > 2 ? unpadded : unpadded + "==".substring(0, padLength));
                } catch (Throwable throwable) {
                    // Invoked method doesn't throw checked exceptions, so safe to cast
                    throw (RuntimeException) throwable;
                }
            }
        };

        private static String encodeToString(byte[] bytes) {
            try {
                return (String) PRINT.invokeExact(bytes);
            } catch (Throwable throwable) {
                // Invoked method doesn't throw checked exceptions, so safe to cast
                throw (RuntimeException) throwable;
            }
        }

        @Override
        public Encoder urlEncoderNoPadding() {
            return URL_ENCODER_NO_PADDING;
        }

        @Override
        public Encoder encoder() {
            return ENCODER;
        }

        @Override
        public Decoder urlDecoder() {
            return URL_DECODER;
        }

        @Override
        public Decoder decoder() {
            return DECODER;
        }
    }
}
