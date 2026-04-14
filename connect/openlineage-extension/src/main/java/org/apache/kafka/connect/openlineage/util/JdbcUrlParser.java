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

package org.apache.kafka.connect.openlineage.util;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses JDBC connection URLs and extracts an OpenLineage-compliant
 * namespace and default database name.
 *
 * <p>The namespace follows the OpenLineage naming convention:
 * {@code <scheme>://<host>:<port>}, for example
 * {@code postgresql://dbhost:5432}.
 *
 * <p>Supported JDBC URL patterns:
 * <ul>
 *   <li>{@code jdbc:postgresql://host:port/dbname}</li>
 *   <li>{@code jdbc:mysql://host:port/dbname}</li>
 *   <li>{@code jdbc:sqlserver://host:port;databaseName=dbname}</li>
 *   <li>{@code jdbc:oracle:thin:@//host:port/service}</li>
 *   <li>{@code jdbc:oracle:thin:@host:port:sid}</li>
 *   <li>{@code jdbc:redshift://host:port/dbname}</li>
 *   <li>{@code jdbc:snowflake://account.snowflakecomputing.com:port/?db=dbname}</li>
 * </ul>
 */
public final class JdbcUrlParser {

    /** Result of parsing a JDBC URL. */
    public static final class JdbcConnectionInfo {
        private final String namespace;
        private final String database;

        public JdbcConnectionInfo(String namespace, String database) {
            this.namespace = namespace;
            this.database = database;
        }

        /** OpenLineage namespace, e.g. {@code postgresql://host:5432}. */
        public String namespace() {
            return namespace;
        }

        /** Database name extracted from the URL, or empty string if unknown. */
        public String database() {
            return database;
        }
    }

    // jdbc:subprotocol://host:port/database...
    private static final Pattern STANDARD_PATTERN =
        Pattern.compile("jdbc:(\\w+)://([^/:?;]+)(?::(\\d+))?(?:/([^?;]*))?");

    // jdbc:sqlserver://host:port;databaseName=xxx
    private static final Pattern SQLSERVER_DB_PATTERN =
        Pattern.compile("databaseName=([^;]+)", Pattern.CASE_INSENSITIVE);

    // jdbc:oracle:thin:@//host:port/service  or  @host:port:sid
    private static final Pattern ORACLE_THIN_PATTERN =
        Pattern.compile("jdbc:oracle:thin:@//(.*?)(?::(\\d+))?/(.+)");
    private static final Pattern ORACLE_SID_PATTERN =
        Pattern.compile("jdbc:oracle:thin:@(.*?):(\\d+):(.+)");

    // jdbc:snowflake://account.snowflakecomputing.com/?db=xxx
    private static final Pattern SNOWFLAKE_DB_PATTERN =
        Pattern.compile("[?&]db=([^&]+)", Pattern.CASE_INSENSITIVE);

    private JdbcUrlParser() {
        // utility class
    }

    /**
     * Parse a JDBC URL into an OpenLineage namespace and database name.
     *
     * @param jdbcUrl the JDBC URL to parse
     * @return parsed connection info, never {@code null}
     */
    public static JdbcConnectionInfo parse(String jdbcUrl) {
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            return new JdbcConnectionInfo("jdbc://unknown", "");
        }

        String url = jdbcUrl.trim();

        // Oracle thin format
        Matcher oracleThin = ORACLE_THIN_PATTERN.matcher(url);
        if (oracleThin.find()) {
            String host = oracleThin.group(1);
            String port = oracleThin.group(2) != null ? oracleThin.group(2) : "1521";
            String service = oracleThin.group(3);
            return new JdbcConnectionInfo("oracle://" + host + ":" + port, service);
        }
        Matcher oracleSid = ORACLE_SID_PATTERN.matcher(url);
        if (oracleSid.find()) {
            String host = oracleSid.group(1);
            String port = oracleSid.group(2);
            String sid = oracleSid.group(3);
            return new JdbcConnectionInfo("oracle://" + host + ":" + port, sid);
        }

        // Standard format: jdbc:subprotocol://host:port/db
        Matcher standard = STANDARD_PATTERN.matcher(url);
        if (standard.find()) {
            String subProtocol = normalizeScheme(standard.group(1).toLowerCase(Locale.ROOT));
            String host = standard.group(2);
            String port = standard.group(3);
            String dbPath = standard.group(4);

            int defaultPort = defaultPort(subProtocol);
            String effectivePort = port != null ? port : String.valueOf(defaultPort);
            String namespace = subProtocol + "://" + host + ":" + effectivePort;

            String database = "";

            // SQL Server: databaseName param
            if ("sqlserver".equals(subProtocol)) {
                Matcher dbMatcher = SQLSERVER_DB_PATTERN.matcher(url);
                if (dbMatcher.find()) {
                    database = dbMatcher.group(1);
                }
            } else if ("snowflake".equals(subProtocol)) {
                // Snowflake: ?db=xxx
                Matcher dbMatcher = SNOWFLAKE_DB_PATTERN.matcher(url);
                if (dbMatcher.find()) {
                    database = dbMatcher.group(1);
                }
            } else if (dbPath != null && !dbPath.isEmpty()) {
                database = dbPath;
            }

            return new JdbcConnectionInfo(namespace, database);
        }

        // Fallback
        return new JdbcConnectionInfo(url, "");
    }

    /**
     * Map JDBC sub-protocol names to OpenLineage naming convention.
     * See https://openlineage.io/docs/spec/naming/
     */
    private static String normalizeScheme(String subProtocol) {
        if ("postgresql".equals(subProtocol)) {
            return "postgres";
        }
        return subProtocol;
    }

    private static int defaultPort(String subProtocol) {
        switch (subProtocol) {
            case "postgres":
            case "postgresql":
                return 5432;
            case "mysql":
                return 3306;
            case "sqlserver":
                return 1433;
            case "oracle":
                return 1521;
            case "redshift":
                return 5439;
            case "snowflake":
                return 443;
            default:
                return 0;
        }
    }
}
