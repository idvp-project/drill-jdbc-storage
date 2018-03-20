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

import com.google.common.base.Joiner;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.LazyJdbcSchema;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.exec.store.AbstractSchema;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Oleg Zinoviev
 * @since 01.08.2017.
 */
class DrillJdbcSchema extends AbstractSchema {

    private final JdbcSchema inner;
    private final JdbcStoragePlugin plugin;
    private final ConcurrentMap<String, DrillJdbcSchema> children = new ConcurrentHashMap<>();
    private final boolean isCatalog;

    DrillJdbcSchema(List<String> parentSchemaPath,
                    String catalog,
                    String schema,
                    JdbcStoragePlugin plugin) {
        super(parentSchemaPath, ObjectUtils.firstNonNull(schema, catalog));
        this.plugin = plugin;
        inner = new LazyJdbcSchema(plugin.getSource(),
                plugin.getDialect(),
                plugin.getConvention(),
                catalog,
                schema);
        this.isCatalog = StringUtils.isNotEmpty(catalog);
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
        return children.computeIfAbsent(name, s -> {
            if (!isCatalog) {
                return null;
            }

            //Поиск новых схем выполняем только для схем-каталогов
            if (plugin.isSchema(getSchemaPath(), s)) {
                return new DrillJdbcSchema(getSchemaPath(), null, s, plugin);
            }
            return null;
        });
    }

    void addSubSchema(String name) {
        DrillJdbcSchema subSchema = children.get(name);
        if (subSchema == null) {
            subSchema = new DrillJdbcSchema(getSchemaPath(), getName(), name, plugin);
            children.putIfAbsent(name, subSchema);
        }
    }

    void setHolder(SchemaPlus plusOfThis) {
        for (DrillJdbcSchema schema : children.values()) {
            SchemaPlus holder = plusOfThis.add(schema.getName(), schema);
            schema.setHolder(holder);
        }
    }

    public String toString() {
        return Joiner.on(".").join(getSchemaPath());
    }

    @Override
    public Table getTable(String name) {
        Table table = inner.getTable(name);
        if (table != null) {
            return table;
        }
        return inner.getTable(name.toUpperCase());

    }


}
