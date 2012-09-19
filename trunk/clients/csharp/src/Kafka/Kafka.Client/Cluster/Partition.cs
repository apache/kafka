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

namespace Kafka.Client.Cluster
{
    using System;
    using System.Globalization;

    /// <summary>
    /// Represents broker partition
    /// </summary>
    internal class Partition : IComparable<Partition>
    {
        /// <summary>
        /// Factory method that instantiates <see cref="Partition"/>  object based on configuration given as string
        /// </summary>
        /// <param name="partition">
        /// The partition info.
        /// </param>
        /// <returns>
        /// Instantiated <see cref="Partition"/>  object
        /// </returns>
        public static Partition ParseFrom(string partition)
        {
            var pieces = partition.Split('-');
            if (pieces.Length != 2)
            {
                throw new ArgumentException("Expected name in the form x-y");
            }

            return new Partition(int.Parse(pieces[0], CultureInfo.InvariantCulture), int.Parse(pieces[1], CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Partition"/> class.
        /// </summary>
        /// <param name="brokerId">
        /// The broker ID.
        /// </param>
        /// <param name="partId">
        /// The partition ID.
        /// </param>
        public Partition(int brokerId, int partId)
        {
            this.BrokerId = brokerId;
            this.PartId = partId;
        }

        /// <summary>
        /// Gets the broker Dd.
        /// </summary>
        public int BrokerId { get; private set; }

        /// <summary>
        /// Gets the partition ID.
        /// </summary>
        public int PartId { get; private set; }

        /// <summary>
        /// Gets broker name as concatanate broker ID and partition ID
        /// </summary>
        public string Name
        {
            get { return this.BrokerId + "-" + this.PartId; }
        }

        /// <summary>
        /// Compares current object with another object of type <see cref="Partition"/>
        /// </summary>
        /// <param name="other">
        /// The other object.
        /// </param>
        /// <returns>
        /// 0 if equals, positive number if greater and negative otherwise
        /// </returns>
        public int CompareTo(Partition other)
        {
            if (this.BrokerId == other.BrokerId)
            {
                return this.PartId - other.PartId;
            }

            return this.BrokerId - other.BrokerId;
        }

        /// <summary>
        /// Gets string representation of current object
        /// </summary>
        /// <returns>
        /// String that represents current object
        /// </returns>
        public override string ToString()
        {
            return "(" + this.BrokerId + "," + this.PartId + ")";
        }

        /// <summary>
        /// Determines whether a given object is equal to the current object
        /// </summary>
        /// <param name="obj">
        /// The other object.
        /// </param>
        /// <returns>
        /// Equality of given and current objects
        /// </returns>
        public override bool Equals(object obj)
        {
            if (obj == null)
            {
                return false;
            }

            var other = obj as Partition;
            if (other == null)
            {
                return false;
            }

            return this.BrokerId == other.BrokerId && this.PartId == other.PartId;
        }

        /// <summary>
        /// Gets hash code of current object
        /// </summary>
        /// <returns>
        /// Hash code
        /// </returns>
        public override int GetHashCode()
        {
            return (31 * (17 + this.BrokerId)) + this.PartId;
        }
    }
}
