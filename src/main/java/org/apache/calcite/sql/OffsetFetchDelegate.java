/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql;

import org.apache.calcite.sql.dialect.MssqlSqlDialect;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.dialect.VerticaSqlDialect;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * @author olegzinovev
 * @since 2019-02-12
 **/
class OffsetFetchDelegate {
    private final SqlDialect dialect;
    private final boolean supportsOffsetFetch;

    OffsetFetchDelegate(final SqlDialect dialect,
                        final DataSource dataSource) throws SQLException {
        this.dialect = dialect;
        this.supportsOffsetFetch = detectOffsetFetchSupport(dataSource);
    }

    /**
     * iDVP Data поддерживает на данный момент 4 субд
     * - MSSQL - поддерживает OFFSET FETCH с 2012 (11.x) версии
     * - Oracle - поддерживает OFFSET FETCH с 12 версии
     * - PostgreSQL - поддерживает OFFSET FETCH с 9 версии,
     *          однако начиная с 7 версии (как минимум) поддерживает LIMIT OFFSET.
     *          Поэтому переопределяем стандартный PostgresqlSqlDialect
     * - Vertica - поддерживает LIMIT OFFSET. Поэтому переопределяем стандартный VerticaSqlDialect
     */
    private boolean detectOffsetFetchSupport(final DataSource dataSource) throws SQLException {


        //noinspection deprecation
        if (!dialect.supportsOffsetFetch()) {
            return false;
        }

        if (dialect instanceof OracleSqlDialect) {
            // Oracle поддерживает OFFSET + FETCH начиная с 12 версии
            try (Connection connection = dataSource.getConnection()) {
                DatabaseMetaData metaData = connection.getMetaData();
                return metaData.getDatabaseMajorVersion() >= 12;
            }
        }

        if (dialect instanceof MssqlSqlDialect) {
            // MS SQL поддерживает OFFSET + FETCH начиная с 2012 (11.x) версии
            try (Connection connection = dataSource.getConnection()) {
                DatabaseMetaData metaData = connection.getMetaData();
                return metaData.getDatabaseMajorVersion() >= 11;
            }
        }

        //noinspection deprecation
        return dialect.supportsOffsetFetch();
    }

    boolean supportsOffsetFetch() {
        return supportsOffsetFetch;
    }

    /**
     * см {@link #supportsOffsetFetch()}
     */
    void unparseOffsetFetch(final SqlWriter writer,
                            final SqlNode offset,
                            final SqlNode fetch) {

        if (dialect instanceof PostgresqlSqlDialect) {
            dialect.unparseFetchUsingLimit(writer, offset, fetch);
        }

        if (dialect instanceof VerticaSqlDialect) {
            dialect.unparseFetchUsingLimit(writer, offset, fetch);
        }

        // default behavior
        dialect.unparseOffsetFetch(writer, offset, fetch);
    }
}
