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
 */
package org.apache.kafka.common.record;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;


public class CompressionCodecsTest {
    @Rule
    public PrintRandomSeed printRule = new PrintRandomSeed();
    private long seed;
    private Random random;

    @Before
    public void setup() {
        seed = System.currentTimeMillis();
        random = new Random(seed);
    }

    @Test
    public void testByteArrayInputStream() throws Exception {
        testCodec(
            new InputStreamWrapper() {
                @Override
                public InputStream wrap(InputStream underlying) throws Exception {
                    //noinspection RedundantCast - serves as assertion
                    return (ByteArrayInputStream) underlying;
                }
            },
            new OutputStreamWrapper() {
                @Override
                public OutputStream wrap(OutputStream underlying) throws Exception {
                    //noinspection RedundantCast - serves as assertion
                    return (ByteArrayOutputStream) underlying;
                }
            });
    }

    @Test
    public void testByteBufferInputStream() throws Exception {
        byte[] blob = generateRandomData();
        ByteBufferInputStream is = new ByteBufferInputStream(ByteBuffer.wrap(blob));
        testCodec(blob, is);
    }

    @Test(expected = AssertionError.class) //vanilla gzip has a bad impl of available()
    public void testVanillaGzip() throws Exception {
        testCodec(
            new InputStreamWrapper() {
                @Override
                public InputStream wrap(InputStream underlying) throws Exception {
                    return new GZIPInputStream(underlying);
                }
            },
            new OutputStreamWrapper() {
                @Override
                public OutputStream wrap(OutputStream underlying) throws Exception {
                    return new GZIPOutputStream(underlying);
                }
            });
    }

    @Test
    public void testKafkaGzip() throws Exception {
        testCodec(
            new InputStreamWrapper() {
                @Override
                public InputStream wrap(InputStream underlying) throws Exception {
                    return new KafkaGZIPInputStream(underlying);
                }
            },
            new OutputStreamWrapper() {
                @Override
                public OutputStream wrap(OutputStream underlying) throws Exception {
                    return new GZIPOutputStream(underlying); //use vanilla
                }
            });
    }

    @Test
    public void testSnappy() throws Exception {
        testCodec(
            new InputStreamWrapper() {
                @Override
                public InputStream wrap(InputStream underlying) throws Exception {
                    return new SnappyInputStream(underlying);
                }
            },
            new OutputStreamWrapper() {
                @Override
                public OutputStream wrap(OutputStream underlying) throws Exception {
                    return new SnappyOutputStream(underlying);
                }
            });
    }

    @Test
    public void testKafkaLz4Legacy() throws Exception {
        testCodec(
            new InputStreamWrapper() {
                @Override
                public InputStream wrap(InputStream underlying) throws Exception {
                    return new KafkaLZ4BlockInputStream(underlying, true);
                }
            },
            new OutputStreamWrapper() {
                @Override
                public OutputStream wrap(OutputStream underlying) throws Exception {
                    return new KafkaLZ4BlockOutputStream(underlying, true);
                }
            });
    }

    @Test
    public void testKafkaLz4() throws Exception {
        testCodec(
            new InputStreamWrapper() {
                @Override
                public InputStream wrap(InputStream underlying) throws Exception {
                    return new KafkaLZ4BlockInputStream(underlying, false);
                }
            },
            new OutputStreamWrapper() {
                @Override
                public OutputStream wrap(OutputStream underlying) throws Exception {
                    return new KafkaLZ4BlockOutputStream(underlying, false);
                }
            });
    }

    public void testCodec(InputStreamWrapper inputStreamWrapper, OutputStreamWrapper outputStreamWrapper) throws Exception {

        //1. generate random data
        byte[] blob = generateRandomData();

        //2. encode (compress) data
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputStream encoderStream = outputStreamWrapper.wrap(baos);
        encoderStream.write(blob);
        encoderStream.close(); //implies flush
        baos.close();
        byte[] encoded = baos.toByteArray();

        //3. create a decode stream reading the compressed data
        ByteArrayInputStream bais = new ByteArrayInputStream(encoded);
        InputStream decoderStream = inputStreamWrapper.wrap(bais);

        testCodec(blob, decoderStream);
    }

    public void testCodec(byte[] expected, InputStream is) throws Exception {
        //read byte by byte and compare with original data
        int available;
        int bytesRead = 0;
        while ((available = is.available()) > 0) {
            int read = is.read();
            //make sure we didnt get -1
            Assert.assertTrue("decoder returned available() = " + available + " but subsequent read() returned " + read, read >= 0);
            //make sure output matches input
            Assert.assertTrue("decoded is longer than original", bytesRead < expected.length);
            Assert.assertTrue("decoded differs from source at offset " + bytesRead, expected[bytesRead] == (byte) read);
            bytesRead++;
        }
        Assert.assertEquals("decoder returned available() = 0 too early after " + bytesRead + " bytes", bytesRead, expected.length);
    }

    private byte[] generateRandomData() {
        int length = 1 + random.nextInt(4096); //[1, 4096]
        byte[] blob = new byte[length];
        random.nextBytes(blob);
        return blob;
    }

    private interface InputStreamWrapper {
        InputStream wrap(InputStream underlying) throws Exception;
    }

    private interface OutputStreamWrapper {
        OutputStream wrap(OutputStream underlying) throws Exception;
    }

    private class PrintRandomSeed extends TestWatcher {
        @Override
        protected void failed(Throwable e, Description description) {
            Assert.fail("random seed was " + seed); //to assist with reproduction
        }
    }
}
