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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.jdbc.LazyJdbcSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.store.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author Oleg Zinoviev
 * @since 01.08.2017.
 */
class JdbcCatalogSchema extends AbstractSchema {

    private final static Logger logger = LoggerFactory.getLogger(JdbcCatalogSchema.class);

    private final JdbcStoragePlugin plugin;
    private final ConcurrentMap<String, DrillJdbcSchema> children = new ConcurrentSkipListMap<>(String::compareToIgnoreCase);
    private final Instant timestamp;
    private Schema rootSchema;

    JdbcCatalogSchema(JdbcStoragePlugin plugin, String name) {
        super(ImmutableList.of(), name);
        this.plugin = plugin;
        this.timestamp = Instant.now();
        init();
    }

    void fill(SchemaPlus plusOfThis) {
        if (children.isEmpty()) {
            init();
        }

        for (DrillJdbcSchema schema : children.values()) {
            plusOfThis.add(schema.getName(), schema);
            schema.setHolder(plusOfThis);
        }
    }


    boolean isEvicted() {
        if (plugin.getConfig().getMetadataLifetime() <= 0) {
            return false;
        }

        return Instant.now()
                .isAfter(
                        timestamp.plus(
                                Duration.ofMillis(plugin.getConfig().getMetadataLifetime()))
                );
    }

    private void init() {
        if (!children.isEmpty()) {
            return;
        }

        synchronized (this) {
            if (!children.isEmpty()) {
                return;
            }

            try (Connection connection = plugin.getSource().getConnection()) {
                try (ResultSet schemas = connection.getMetaData().getSchemas()) {
                    while (schemas.next()) {
                        final String schemaName = schemas.getString(1);
                        final String catalogName = schemas.getString(2);

                        if (StringUtils.isNotEmpty(catalogName)) {
                            children.computeIfAbsent(catalogName, c -> new DrillJdbcSchema(getSchemaPath(),
                                    c,
                                    null,
                                    plugin))
                                    .addSubSchema(schemaName);
                        }

                        children.put(schemaName, new DrillJdbcSchema(
                                getSchemaPath(),
                                null,
                                schemaName,
                                plugin));
                    }
                }
            } catch (SQLException e) {
                logger.error("Cannot load schema list", e);
                throw UserException.internalError(e)
                        .build(logger);
            }
        }
    }

    @Override
    public String getTypeName() {
        return JdbcStorageConfig.NAME;
    }

    @Override
    public DrillJdbcSchema getSubSchema(String name) {
        return children.computeIfAbsent(name, s -> {
            if (plugin.isSchema(getSchemaPath(), s)) {
                return new DrillJdbcSchema(getSchemaPath(), null, s, plugin);
            }
            return null;
        });
    }

    @Override
    public Table getTable(String name) {
        Schema schema = getRootSchema();

        if (schema != null) {
            try {
                Table t = schema.getTable(name);
                if (t != null) {
                    return t;
                }
                return schema.getTable(name.toUpperCase());
            } catch (RuntimeException e) {
                JdbcStoragePlugin.logger.warn("Failure while attempting to read table '{}' from JDBC source.", name, e);

            }
        }

        // no table was found.
        return null;
    }

    private Schema getRootSchema() {
        if (rootSchema == null) {
            synchronized (this) {
                if (rootSchema == null) {
                    this.rootSchema = new LazyJdbcSchema(plugin.getSource(),
                            plugin.getDialect(),
                            plugin.getConvention(),
                            null,
                            null);
                }
            }
        }
        return rootSchema;
    }
}
