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
package kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

class DualWriteExample {
    public static void main(String[] arg) {
        // TODO: implement some test logic using DualWriter class
    }

    /**
     * Example class that writes some kv pairs to a database, the last value overwrites previous,
     * if value is null, the key is deleted.  The kv pairs are atomically produced to Kafka, so all
     * changes to the database are also available from a Kafka topic.
     * <p>
     * Most of the example logic would work with any application logic and any SQL database, the
     * example-specific logic (that can be replaced with application logic) is marked with comments.
     * <p>
     * This example assumes that the database has the following tables:
     *  - kafka_txn_state(txn_id varchar(255), prepared_txn_state varchar(max)): utility table used by the dual write recipe
     *  - app_kv_pairs(key varchar(255), value varchar(max)): application data table
     */
    public static class DualWriter {
        // Kafka configuration
        final String bootstrapServers;
        final String transactionalId;
        final String topic;

        // SQL database connection
        final Connection sqlConnection;

        KafkaProducer<String, String> kafkaProducer = null;

        /**
         * Constructor
         * @param bootstrapServers The boostrap servers for Kafka producer
         * @param transactionalId The transactional id for Kafka producer
         * @param topic The Kafka topic to produce changes to
         * @param sqlConnection The JDBC connection to the database
         */
        public DualWriter(String bootstrapServers,
                          String transactionalId,
                          String topic,
                          Connection sqlConnection) {
            this.bootstrapServers = bootstrapServers;
            this.transactionalId = transactionalId;
            this.topic = topic;

            this.sqlConnection = sqlConnection;
        }

        /**
         * Update the database and log changes to Kafka
         * @param kvs The kv pairs to write to database and Kafka, null values mean to delete key
         */
        public void update(Map<String, String> kvs) throws SQLException {
            maybeCreateKafkaProducer();

            // Need to use transactions.  This makes it so that the first statement
            // (that runs in getOrCreatePreparedTxnState) starts the database transaction
            sqlConnection.setAutoCommit(false);

            // Get the transaction state from the database.  Note, that this operation takes the update
            // lock on the transactional record that is held until the end of the transaction
            // and protects integrity from various race conditions.
            KafkaProducer.PreparedTxnState prevPreparedTxnState = getOrCreatePreparedTxnState();

            // Complete the previous transaction (if there was any).  This takes care of failure cases
            // that didn't finish via the happy path at the end of this function.
            kafkaProducer.completeTransaction(prevPreparedTxnState);

            // Start a new Kafka transaction.  Once we have both Kafka and DB transactions opened,
            // we can write data, it doesn't matter whether we update Kafka first or DB first.
            kafkaProducer.beginTransaction();

            // === Begin application-specific example logic  ===
            for (Map.Entry<String, String> kv : kvs.entrySet()) {
                kafkaProducer.send(new ProducerRecord<>(topic, kv.getKey(), kv.getValue()));

                if (kv.getValue() == null)
                    delete(kv.getKey());
                else
                    put(kv.getKey(), kv.getValue());
            }
            // === End application-specific example logic ===

            // We're done with data updates, do the commits.  We prepare Kafka transaction and commit
            // the prepared transaction state along with the application data.  This way the outcome
            // of the database transaction defines the outcome of the dual write.
            KafkaProducer.PreparedTxnState preparedTxnState = kafkaProducer.prepareTransaction();
            PreparedStatement updateTxnState = sqlConnection.prepareStatement(
                    "UPDATE kafka_txn_state SET prepared_txn_state=? WHERE txn_id=?");
            updateTxnState.setString(1, preparedTxnState.toString());
            updateTxnState.setString(2, transactionalId);
            updateTxnState.executeUpdate();
            sqlConnection.commit();

            // At this point the dual write is committed, tell Kafka that it's done.
            try {
                kafkaProducer.commitTransaction();
            } catch (ProducerFencedException e) {
                // In a split brain case, our transaction may be completed by a new instance of
                // this application.  We'll give an opportunity to the caller to continue
                // processing (if it decides so) by resetting kafka producer.
                kafkaProducer = null;
                throw e;
            }
        }

        /**
         * Synchronize Kafka state with DB state.  Note that if the database commit failed
         * or Kafka commit failed, it doesn't mean that the transaction failed.
         * The actual outcome needs to be retrieved from the database and can be rolled
         * forward (or back) using the flush method.
         */
        public void flush() throws SQLException {
            maybeCreateKafkaProducer();

            // Need to use transactions.  This makes it so that the first statement
            // (that runs in getOrCreatePreparedTxnState) starts the database transaction
            sqlConnection.setAutoCommit(false);

            // Get the transaction state from the database.  Note, that this operation takes the update
            // lock on the transactional record that is held until the end of the transaction
            // and protects integrity from various race conditions.
            KafkaProducer.PreparedTxnState prevPreparedTxnState = getOrCreatePreparedTxnState();

            // Complete the previous transaction (if there was any).  This takes care of failure cases
            // that didn't finish via the happy path at the end of this function.
            kafkaProducer.completeTransaction(prevPreparedTxnState);

            // Release the lock on the transaction state record.
            sqlConnection.rollback();
        }

        /**
         * Get the prepared txn state from the utility table.  This is done as part of the transaction
         * and takes the update lock on the transaction state record (therefore FOR UPDATE is important).
         * This lock protects the atomicity of database and Kafka updates from:
         * <li>zombies (process restarted but the DB transaction started by previous incarnation is still in progress)</li>
         * <li>split brains (new process started before old process terminated)</li>
         * @return The prepared transaction state, deserialized from the database record
         */
        private KafkaProducer.PreparedTxnState getOrCreatePreparedTxnState() throws SQLException {
            PreparedStatement selectTxnState = sqlConnection.prepareStatement(
                    "SELECT prepared_txn_state FROM kafka_txn_state WHERE txn_id=? FOR UPDATE");
            selectTxnState.setString(1, transactionalId);
            ResultSet resultSet = selectTxnState.executeQuery();
            if (resultSet.next()) {
                // Normal case, we generally expect to have a transactional state from the previous transaction.
                return new KafkaProducer.PreparedTxnState(resultSet.getString(1));
            }

            // This is the first time this transactional id is used with this database, we need to insert
            // the state record.
            KafkaProducer.PreparedTxnState newState = new KafkaProducer.PreparedTxnState();

            PreparedStatement insertTxnState = sqlConnection.prepareStatement(
                    "INSERT INTO kafka_txn_state(txn_id, prepared_txn_state) VALUES (?, ?)");
            insertTxnState.setString(1, transactionalId);
            insertTxnState.setString(2, newState.toString());

            if (insertTxnState.executeUpdate() <= 0) {
                // Failed to insert probably because of a race condition, try again.
                return getOrCreatePreparedTxnState();
            }

            return newState;
        }

        /**
         * Helper function to configure and create producer.
         */
        private void maybeCreateKafkaProducer() {
            if (kafkaProducer == null) {
                Properties props = new Properties();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
                kafkaProducer = new KafkaProducer<>(props);

                // Initialize Kafka producer and ask it to keep prepared transaction, if any.
                kafkaProducer.initTransactions(true);
            }
        }

        /**
         * EXAMPLE - application-specific database logic, put the kv-pair to the application table.
         * @param key The key of the kv-pair
         * @param value The value of the kv-pair
         */
        private void put(String key, String value) throws SQLException {
            PreparedStatement updateKvPair = sqlConnection.prepareStatement(
                    "UPDATE app_kv_pairs SET value=? WHERE key=?");
            updateKvPair.setString(1, value);
            updateKvPair.setString(2, key);

            if (updateKvPair.executeUpdate() <= 0) {
                PreparedStatement insertKvPair = sqlConnection.prepareStatement(
                        "INSERT INTO app_kv_pairs(key, value) VALUES(?, ?");
                insertKvPair.setString(1, key);
                insertKvPair.setString(2, value);
                insertKvPair.executeUpdate();
            }
        }

        /**
         * EXAMPLE - application-specific data logic, delete the kv-pair with the given key from the table.
         * @param key The key to be deleted
         */
        private void delete(String key) throws SQLException {
            PreparedStatement deleteKvPair = sqlConnection.prepareStatement(
                    "DELETE FROM app_kv_pairs WHERE key=?");
            deleteKvPair.setString(1, key);
            deleteKvPair.executeUpdate();
        }
    }
}
