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
package org.apache.drill.exec.store.idvp.jdbc;

import org.apache.commons.dbcp.*;
import org.apache.commons.pool.KeyedObjectPoolFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author Oleg Zinoviev
 * @since 11.09.2017
 **/
public class JdbcBasicDataSource extends BasicDataSource {

    @Override
    protected ConnectionFactory createConnectionFactory() throws SQLException {
        boolean testOnBorrow = getTestOnBorrow();
        boolean testOnReturn = getTestOnReturn();
        boolean testWhileIdle = getTestWhileIdle();

        ConnectionFactory connectionFactory = super.createConnectionFactory();

        setTestOnBorrow(testOnBorrow);
        setTestOnReturn(testOnReturn);
        setTestWhileIdle(testWhileIdle);

        return connectionFactory;
    }

    @Override
    protected void createPoolableConnectionFactory(ConnectionFactory driverConnectionFactory,
                                                   KeyedObjectPoolFactory statementPoolFactory,
                                                   AbandonedConfig configuration) throws SQLException {
        ConnFactory connectionFactory;

        try {
            connectionFactory = new ConnFactory(
                    driverConnectionFactory,
                    this.connectionPool,
                    statementPoolFactory,
                    this.validationQuery,
                    this.validationQueryTimeout,
                    this.connectionInitSqls,
                    this.defaultReadOnly,
                    this.defaultAutoCommit,
                    this.defaultTransactionIsolation,
                    this.defaultCatalog, configuration);
            validateConnectionFactory(connectionFactory);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            //noinspection deprecation
            throw new SQLNestedException("Cannot create PoolableConnectionFactory (" + e.getMessage() + ")", e);
        }
    }

    public static class ConnFactory extends PoolableConnectionFactory {

        ConnFactory(ConnectionFactory driverConnectionFactory,
                    GenericObjectPool connectionPool,
                    KeyedObjectPoolFactory statementPoolFactory,
                    String validationQuery,
                    int validationQueryTimeout,
                    List connectionInitSqls,
                    Boolean defaultReadOnly,
                    boolean defaultAutoCommit,
                    int defaultTransactionIsolation,
                    String defaultCatalog,
                    AbandonedConfig configuration) {
            super(driverConnectionFactory,
                    connectionPool,
                    statementPoolFactory,
                    validationQuery,
                    validationQueryTimeout,
                    connectionInitSqls,
                    defaultReadOnly,
                    defaultAutoCommit,
                    defaultTransactionIsolation,
                    defaultCatalog,
                    configuration);
        }

        @Override
        public void validateConnection(Connection conn) throws SQLException {
            if (conn.isClosed()) {
                throw new SQLException("validateConnection: connection closed");
            } else {
                if (this._validationQueryTimeout > 0) {
                    if (!conn.isValid(this._validationQueryTimeout)) {
                        throw new SQLException("validateConnection: connection not valid");
                    }
                }

                super.validateConnection(conn);

            }
        }
    }
}
