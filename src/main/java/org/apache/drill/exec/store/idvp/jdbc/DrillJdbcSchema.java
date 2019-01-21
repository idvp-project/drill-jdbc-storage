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

import org.apache.drill.shaded.guava.com.google.common.base.Joiner;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.LazyJdbcSchema;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.store.AbstractSchema;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author Oleg Zinoviev
 * @since 01.08.2017.
 */
class DrillJdbcSchema extends AbstractSchema {

    private final JdbcSchema inner;
    private final JdbcStoragePlugin plugin;
    private final ConcurrentMap<String, Object> children = new ConcurrentSkipListMap<>(String::compareToIgnoreCase);

    DrillJdbcSchema(List<String> parentSchemaPath,
                    String name,
                    JdbcStoragePlugin plugin) {
        super(parentSchemaPath, name);
        this.plugin = plugin;
        String catalog = null;
        String schema = null;

        if (getSchemaPath().size() == 2) {
            schema = getSchemaPath().get(1);
        }

        if (getSchemaPath().size() == 3) {
            catalog = getSchemaPath().get(1);
            schema = getSchemaPath().get(2);
        }

        inner = new LazyJdbcSchema(plugin.getSource(), plugin.getDialect(), plugin.getConvention(), catalog, schema);
    }

    @Override
    public String getTypeName() {
        return JdbcStorageConfig.NAME;
    }

    @Override
    public Collection<Function> getFunctions(String name) {
        return inner.getFunctions(name);
    }

    @Override
    public Set<String> getFunctionNames() {
        return inner.getFunctionNames();
    }

    @Override
    public DrillJdbcSchema getSubSchema(String name) {
        Object schema = children.computeIfAbsent(name, n -> new DrillJdbcSchema(getSchemaPath(), n, plugin));
        if (schema instanceof DrillJdbcSchema) {
            return (DrillJdbcSchema) schema;
        }

        return null;
    }

    void setHolder(SchemaPlus plusOfThis) {
        for (String s : getSubSchemaNames()) {
            DrillJdbcSchema inner = getSubSchema(s);
            SchemaPlus holder = plusOfThis.add(s, inner);
            inner.setHolder(holder);
        }
    }

    public String toString() {
        return Joiner.on(".").join(getSchemaPath());
    }

    @Override
    public Table getTable(String name) {
        Object table = children.computeIfAbsent(name, inner::getTable);
        if (table instanceof Table) {
            return (Table) table;
        }

        return null;
    }

}
