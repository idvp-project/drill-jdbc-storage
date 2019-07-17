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

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.jdbc.LazyJdbcSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.store.AbstractSchema;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author Oleg Zinoviev
 * @since 01.08.2017.
 */
class JdbcCatalogSchema extends AbstractSchema {

    private final JdbcStoragePlugin plugin;
    private final ConcurrentMap<String, Object> children = new ConcurrentSkipListMap<>(String::compareToIgnoreCase);
    private LazyJdbcSchema rootSchema;

    JdbcCatalogSchema(JdbcStoragePlugin plugin, String name) {
        super(ImmutableList.of(), name);
        this.plugin = plugin;

    }

    void setHolder(SchemaPlus plusOfThis) {
        for (String s : getSubSchemaNames()) {
            DrillJdbcSchema inner = getSubSchema(s);
            SchemaPlus holder = plusOfThis.add(s, inner);
            inner.setHolder(holder);
        }
    }

    @Override
    public String getTypeName() {
        return JdbcStorageConfig.NAME;
    }

    @Override
    public DrillJdbcSchema getSubSchema(String name) {
        Object schema = children.computeIfAbsent(name, n -> new DrillJdbcSchema(getSchemaPath(), n, plugin));
        if (schema instanceof DrillJdbcSchema) {
            return (DrillJdbcSchema) schema;
        }

        return null;
    }

    @Override
    public Table getTable(String name) {
        Object table = children.computeIfAbsent(name, n -> {
            Schema schema = getRootSchema();
            if (schema != null) {
                Table t = schema.getTable(name);
                if (t != null) {
                    return t;
                }
                return schema.getTable(name.toUpperCase());
            }

            return null;
        });

        if (table instanceof Table) {
            return (Table) table;
        }

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
