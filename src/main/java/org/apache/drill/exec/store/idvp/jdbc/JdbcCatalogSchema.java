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
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.store.AbstractSchema;

class JdbcCatalogSchema extends AbstractSchema {

    private final JdbcStoragePlugin plugin;

    JdbcCatalogSchema(JdbcStoragePlugin plugin, String name) {
        super(ImmutableList.<String>of(), name);
        this.plugin = plugin;
    }

    void setHolder(SchemaPlus plusOfThis) {
        for (String s : getSubSchemaNames()) {
            CapitalizingJdbcSchema inner = getSubSchema(s);
            SchemaPlus holder = plusOfThis.add(s, inner);
            inner.setHolder(holder);
        }
    }

    @Override
    public String getTypeName() {
        return JdbcStorageConfig.NAME;
    }

    @Override
    public CapitalizingJdbcSchema getSubSchema(String name) {
        return new CapitalizingJdbcSchema(getSchemaPath(), name, plugin);
    }

    @Override
    public Table getTable(String name) {
        Schema schema = getDefaultSchema();

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
}
