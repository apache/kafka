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

namespace Kafka.Client
{
    using System;
    using System.IO;
    using System.Net.Sockets;
    using System.Threading;
    using Kafka.Client.Producers.Async;
    using Kafka.Client.Requests;
    using Kafka.Client.Utils;

    /// <summary>
    /// Manages connections to the Kafka.
    /// </summary>
    public class KafkaConnection : IDisposable
    {
        /// <summary>
        /// TCP client that connects to the server.
        /// </summary>
        private readonly TcpClient client;

        private volatile bool disposed;

        /// <summary>
        /// Initializes a new instance of the KafkaConnection class.
        /// </summary>
        /// <param name="server">The server to connect to.</param>
        /// <param name="port">The port to connect to.</param>
        public KafkaConnection(string server, int port)
        {
            Server = server;
            Port = port;

            // connection opened
            client = new TcpClient(server, port);
        }

        /// <summary>
        /// Gets the server to which the connection is to be established.
        /// </summary>
        public string Server { get; private set; }
        
        /// <summary>
        /// Gets the port to which the connection is to be established.
        /// </summary>
        public int Port { get; private set; }

        /// <summary>
        /// Readds data from the server.
        /// </summary>
        /// <remarks>
        /// Defauls the amount of time that a read operation blocks waiting for data to <see cref="Timeout.Infinite"/>.
        /// </remarks>
        /// <param name="size">The number of bytes to read from the server.</param>
        /// <returns>The data read from the server as a byte array.</returns>
        public byte[] Read(int size)
        {
            this.EnsuresNotDisposed();
            return Read(size, Timeout.Infinite);
        }

        /// <summary>
        /// Readds data from the server.
        /// </summary>
        /// <param name="size">The number of bytes to read from the server.</param>
        /// <param name="readTimeout">The amount of time that a read operation blocks waiting for data.</param>
        /// <returns>The data read from the server as a byte array.</returns>
        public byte[] Read(int size, int readTimeout)
        {
            this.EnsuresNotDisposed();
            byte[] bytes;
            NetworkStream stream = client.GetStream();
            stream.ReadTimeout = readTimeout;
            int numberOfTries = 0;

            int readSize = size;
            if (client.ReceiveBufferSize < size)
            {
                readSize = client.ReceiveBufferSize;
            }

            using (var ms = new MemoryStream())
            {
                var bytesToRead = new byte[client.ReceiveBufferSize];

                while (true)
                {
                    int numberOfBytesRead = stream.Read(bytesToRead, 0, readSize);
                    if (numberOfBytesRead > 0)
                    {
                        ms.Write(bytesToRead, 0, numberOfBytesRead);
                    }

                    if (ms.Length >= size)
                    {
                        break;
                    }

                    if (numberOfBytesRead == 0)
                    {
                        if (numberOfTries >= 1000)
                        {
                            break;
                        }

                        numberOfTries++;
                        Thread.Sleep(10);
                    }
                }

                bytes = new byte[ms.Length];
                ms.Seek(0, SeekOrigin.Begin);
                ms.Read(bytes, 0, (int)ms.Length);
            }

            return bytes;
        }

        /// <summary>
        /// Writes a producer request to the server asynchronously.
        /// </summary>
        /// <param name="request">The request to make.</param>
        public void BeginWrite(ProducerRequest request)
        {
            this.EnsuresNotDisposed();
            Guard.Assert<ArgumentNullException>(() => request != null);
            NetworkStream stream = client.GetStream();
            byte[] data = request.RequestBuffer.GetBuffer();
            stream.BeginWrite(data, 0, data.Length, asyncResult => ((NetworkStream)asyncResult.AsyncState).EndWrite(asyncResult), stream);
        }
        
        /// <summary>
        /// Writes a producer request to the server asynchronously.
        /// </summary>
        /// <param name="request">The request to make.</param>
        /// <param name="callback">The code to execute once the message is completely sent.</param>
        /// <remarks>
        /// Do not dispose connection till callback is invoked, 
        /// otherwise underlying network stream will be closed.
        /// </remarks>
        public void BeginWrite(ProducerRequest request, MessageSent<ProducerRequest> callback)
        {
            this.EnsuresNotDisposed();
            Guard.Assert<ArgumentNullException>(() => request != null);
            if (callback == null)
            {
                this.BeginWrite(request);
                return;
            }

            NetworkStream stream = client.GetStream();
            var ctx = new RequestContext<ProducerRequest>(stream, request);

            byte[] data = request.RequestBuffer.GetBuffer();
            stream.BeginWrite(
                data,
                0,
                data.Length,
                delegate(IAsyncResult asyncResult)
                    {
                        var context = (RequestContext<ProducerRequest>)asyncResult.AsyncState;
                        callback(context);
                        context.NetworkStream.EndWrite(asyncResult);
                    },
                ctx);
        }

        /// <summary>
        /// Writes a producer request to the server.
        /// </summary>
        /// <remarks>
        /// Write timeout is defaulted to infitite.
        /// </remarks>
        /// <param name="request">The <see cref="ProducerRequest"/> to send to the server.</param>
        public void Write(ProducerRequest request)
        {
            this.EnsuresNotDisposed();
            Guard.Assert<ArgumentNullException>(() => request != null);
            this.Write(request.RequestBuffer.GetBuffer(), Timeout.Infinite);
        }

        /// <summary>
        /// Writes a multi-producer request to the server.
        /// </summary>
        /// <remarks>
        /// Write timeout is defaulted to infitite.
        /// </remarks>
        /// <param name="request">The <see cref="MultiProducerRequest"/> to send to the server.</param>
        public void Write(MultiProducerRequest request)
        {
            this.EnsuresNotDisposed();
            Guard.Assert<ArgumentNullException>(() => request != null);
            this.Write(request.RequestBuffer.GetBuffer(), Timeout.Infinite);
        }

        /// <summary>
        /// Writes data to the server.
        /// </summary>
        /// <param name="data">The data to write to the server.</param>
        /// <param name="writeTimeout">The amount of time that a write operation blocks waiting for data.</param>
        private void Write(byte[] data, int writeTimeout)
        {
            NetworkStream stream = this.client.GetStream();
            stream.WriteTimeout = writeTimeout;

            // Send the message to the connected TcpServer. 
            stream.Write(data, 0, data.Length);
        }

        /// <summary>
        /// Writes a fetch request to the server.
        /// </summary>
        /// <remarks>
        /// Write timeout is defaulted to infitite.
        /// </remarks>
        /// <param name="request">The <see cref="FetchRequest"/> to send to the server.</param>
        public void Write(FetchRequest request)
        {
            this.EnsuresNotDisposed();
            Guard.Assert<ArgumentNullException>(() => request != null);
            this.Write(request.RequestBuffer.GetBuffer(), Timeout.Infinite);
        }

        /// <summary>
        /// Writes a multifetch request to the server.
        /// </summary>
        /// <remarks>
        /// Write timeout is defaulted to infitite.
        /// </remarks>
        /// <param name="request">The <see cref="MultiFetchRequest"/> to send to the server.</param>
        public void Write(MultiFetchRequest request)
        {
            this.EnsuresNotDisposed();
            Guard.Assert<ArgumentNullException>(() => request != null);
            this.Write(request.RequestBuffer.GetBuffer(), Timeout.Infinite);
        }

        /// <summary>
        /// Writes a offset request to the server.
        /// </summary>
        /// <remarks>
        /// Write timeout is defaulted to infitite.
        /// </remarks>
        /// <param name="request">The <see cref="OffsetRequest"/> to send to the server.</param>
        public void Write(OffsetRequest request)
        {
            this.EnsuresNotDisposed();
            Guard.Assert<ArgumentNullException>(() => request != null);
            this.Write(request.RequestBuffer.GetBuffer(), Timeout.Infinite);
        }

        /// <summary>
        /// Close the connection to the server.
        /// </summary>
        public void Dispose()
        {
            if (this.disposed)
            {
                return;
            }

            this.disposed = true;
            if (this.client != null)
            {
                this.client.GetStream().Close();
                this.client.Close();
            }
        }

        /// <summary>
        /// Ensures that object was not disposed
        /// </summary>
        private void EnsuresNotDisposed()
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException(this.GetType().Name);
            }
        }
    }
}
