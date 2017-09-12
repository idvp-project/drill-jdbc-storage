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

import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcJoin;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class JdbcStoragePlugin extends AbstractStoragePlugin {

    private final JdbcStorageConfig config;
    private final String name;

    private BasicDataSource source;
    private SqlDialect dialect;
    private DrillJdbcConvention convention;


    public JdbcStoragePlugin(JdbcStorageConfig config, @SuppressWarnings("unused") DrillbitContext context, String name) {
        this.config = config;
        this.name = name;
    }

    /**
     * Returns whether a condition is supported by {@link JdbcJoin}.
     *
     * <p>Corresponds to the capabilities of
     * {@link JdbcJoin#convertConditionToSqlNode}.
     *
     * @param node Condition
     * @return Whether condition is supported
     */
    @SuppressWarnings("unused")
    private static boolean canJoinOnCondition(RexNode node) {
        final List<RexNode> operands;
        switch (node.getKind()) {
            case AND:
            case OR:
                operands = ((RexCall) node).getOperands();
                for (RexNode operand : operands) {
                    if (!canJoinOnCondition(operand)) {
                        return false;
                    }
                }
                return true;

            case EQUALS:
            case IS_NOT_DISTINCT_FROM:
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                operands = ((RexCall) node).getOperands();
                if ((operands.get(0) instanceof RexInputRef)
                        && (operands.get(1) instanceof RexInputRef)) {
                    return true;
                }
                // fall through

            default:
                return false;
        }
    }

    @Override
    public void registerSchemas(SchemaConfig config, SchemaPlus parent) {
        JdbcCatalogSchema schema = new JdbcCatalogSchema(this, name);
        SchemaPlus holder = parent.add(name, schema);
        schema.setHolder(holder);
    }

    @Override
    public JdbcStorageConfig getConfig() {
        return config;
    }

    String getName() {
        return this.name;
    }

    @Override
    public boolean supportsRead() {
        return true;
    }

    DataSource getSource() {

        if (source == null) {
            synchronized (this) {
                if (source == null) {
                    BasicDataSource source = new JdbcBasicDataSource();
                    source.setDriverClassName(config.getDriver());
                    source.setUrl(config.getUrl());

                    if (config.getUsername() != null) {
                        source.setUsername(config.getUsername());
                    }

                    if (config.getPassword() != null) {
                        source.setPassword(config.getPassword());
                    }

                    source.setMaxActive(config.getConnectionPoolSize());
                    if (config.getConnectionValidationTimeout() > 0) {
                        int timeoutInSeconds = Math.max(config.getConnectionValidationTimeout() / 1000, 1);

                        source.setValidationQueryTimeout(timeoutInSeconds);
                        source.setTestOnBorrow(true);
                        source.setTestWhileIdle(true);
                    }

                    if (config.getConnectionEvictionPeriod() > 0
                            && config.getConnectionValidationTimeout() > 0) {
                        source.setTimeBetweenEvictionRunsMillis(config.getConnectionEvictionPeriod());
                        source.setMinEvictableIdleTimeMillis(config.getConnectionEvictionTimeout());
                        source.setNumTestsPerEvictionRun(source.getMaxIdle());
                        source.setTestWhileIdle(true);
                    }
                    this.source = source;
                }
            }
        }

        return source;

    }

    SqlDialect getDialect() {
        if (dialect == null) {
            synchronized (this) {
                if (dialect == null) {
                    if (!config.isUseStandardDialect()) {
                        try (Connection connection = getSource().getConnection()) {
                            this.dialect = JdbcSqlDialect.create(connection.getMetaData());
                        } catch (SQLException e) {
                            throw new IllegalStateException("Cannot connect to database", e);
                        }
                    } else {
                        this.dialect = JdbcSchema.createDialect(getSource());
                    }
                }
            }
        }

        return dialect;
    }

    DrillJdbcConvention getConvention() {
        if (convention == null) {
            synchronized (this) {
                if (convention == null) {
                    this.convention = new DrillJdbcConvention(this, getDialect(), name);
                }
            }
        }

        return convention;
    }

    @Override
    public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> columns)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("deprecation")
    @Override
    public Set<RelOptRule> getPhysicalOptimizerRules(OptimizerRulesContext context) {
        try {
            return getConvention().getRules();
        } catch (Exception e) {
            logger.error("JdbcStoragePlugin.getPhysicalOptimizerRules", e);
            return Collections.emptySet();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (source != null) {
            source.close();
        }
    }
}
