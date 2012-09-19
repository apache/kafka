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

namespace Kafka.Client.ZooKeeperIntegration.Events
{
    using System.Linq;
    using log4net;

    /// <summary>
    /// Represents methods that will handle a ZooKeeper data events  
    /// </summary>
    internal class DataChangedEventItem
    {
        private readonly ILog logger;
        private ZooKeeperClient.ZooKeeperEventHandler<ZooKeeperDataChangedEventArgs> dataChanged;
        private ZooKeeperClient.ZooKeeperEventHandler<ZooKeeperDataChangedEventArgs> dataDeleted;

        /// <summary>
        /// Occurs when znode data changes
        /// </summary>
        public event ZooKeeperClient.ZooKeeperEventHandler<ZooKeeperDataChangedEventArgs> DataChanged
        {
            add
            {
                this.dataChanged -= value;
                this.dataChanged += value;
            }

            remove
            {
                this.dataChanged -= value;
            }
        }

        /// <summary>
        /// Occurs when znode data deletes
        /// </summary>
        public event ZooKeeperClient.ZooKeeperEventHandler<ZooKeeperDataChangedEventArgs> DataDeleted
        {
            add
            {
                this.dataDeleted -= value;
                this.dataDeleted += value;
            }

            remove
            {
                this.dataDeleted -= value;
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DataChangedEventItem"/> class.
        /// </summary>
        /// <param name="logger">
        /// The logger.
        /// </param>
        /// <remarks>
        /// Should use external logger to keep same format of all event logs
        /// </remarks>
        public DataChangedEventItem(ILog logger)
        {
            this.logger = logger;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DataChangedEventItem"/> class.
        /// </summary>
        /// <param name="logger">
        /// The logger.
        /// </param>
        /// <param name="changedHandler">
        /// The changed handler.
        /// </param>
        /// <param name="deletedHandler">
        /// The deleted handler.
        /// </param>
        /// <remarks>
        /// Should use external logger to keep same format of all event logs
        /// </remarks>
        public DataChangedEventItem(
            ILog logger,
            ZooKeeperClient.ZooKeeperEventHandler<ZooKeeperDataChangedEventArgs> changedHandler,
            ZooKeeperClient.ZooKeeperEventHandler<ZooKeeperDataChangedEventArgs> deletedHandler)
        {
            this.logger = logger;
            this.DataChanged += changedHandler;
            this.DataDeleted += deletedHandler;
        }

        /// <summary>
        /// Invokes subscribed handlers for ZooKeeeper data changes event
        /// </summary>
        /// <param name="e">
        /// The event data.
        /// </param>
        public void OnDataChanged(ZooKeeperDataChangedEventArgs e)
        {
            var handlers = this.dataChanged;
            if (handlers == null)
            {
                return;
            }

            foreach (var handler in handlers.GetInvocationList())
            {
                this.logger.Debug(e + " sent to " + handler.Target);
            }

            handlers(e);
        }

        /// <summary>
        /// Invokes subscribed handlers for ZooKeeeper data deletes event
        /// </summary>
        /// <param name="e">
        /// The event data.
        /// </param>
        public void OnDataDeleted(ZooKeeperDataChangedEventArgs e)
        {
            var handlers = this.dataDeleted;
            if (handlers == null)
            {
                return;
            }

            foreach (var handler in handlers.GetInvocationList())
            {
                this.logger.Debug(e + " sent to " + handler.Target);
            }

            handlers(e);
        }

        /// <summary>
        /// Gets the total count of subscribed handlers
        /// </summary>
        public int TotalCount
        {
            get
            {
                return (this.dataChanged != null ? this.dataChanged.GetInvocationList().Count() : 0) +
                    (this.dataDeleted != null ? this.dataDeleted.GetInvocationList().Count() : 0);
            }
        }
    }
}
