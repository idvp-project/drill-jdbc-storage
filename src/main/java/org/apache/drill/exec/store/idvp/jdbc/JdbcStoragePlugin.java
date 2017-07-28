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

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.idvp.jdbc.rules.DrillJdbcConvention;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class JdbcStoragePlugin extends AbstractStoragePlugin {

    private final JdbcStorageConfig config;
    private final DataSource source;
    private final String name;
    private final SqlDialect dialect;
    private final DrillJdbcConvention convention;


    public JdbcStoragePlugin(JdbcStorageConfig config, @SuppressWarnings("unused") DrillbitContext context, String name) {
        this.config = config;
        this.name = name;
        BasicDataSource source = new BasicDataSource();
        source.setDriverClassName(config.getDriver());
        source.setUrl(config.getUrl());

        if (config.getUsername() != null) {
            source.setUsername(config.getUsername());
        }

        if (config.getPassword() != null) {
            source.setPassword(config.getPassword());
        }

        this.source = source;
        this.dialect = JdbcSchema.createDialect(source);
        this.convention = new DrillJdbcConvention(this, dialect, name);
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
        return source;
    }

    SqlDialect getDialect() {
        return dialect;
    }

    DrillJdbcConvention getConvention() {
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
        return convention.getRules();
    }
}
