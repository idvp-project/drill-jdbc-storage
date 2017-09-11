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
package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDialect;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LazyJdbcSchema extends JdbcSchema {

    private final Map<String, Table> tables = new ConcurrentHashMap<>();
    private final String catalog;
    private final String schema;

    public LazyJdbcSchema(DataSource dataSource, SqlDialect dialect, JdbcConvention convention, String catalog, String schema) {
        super(dataSource, dialect, convention, catalog, schema);
        this.catalog = catalog;
        this.schema = schema;
    }

    @Override
    public Table getTable(String name) {
        if (tables.containsKey(name)) {
            return tables.get(name);
        }

        Table table = loadTable(name);
        tables.put(name, table);
        return table;
    }

    private Table loadTable(String name) {
        return new JdbcTable(this, catalog, schema, name, TableType.TABLE);
    }

    @Override
    RelProtoDataType getRelDataType(DatabaseMetaData metaData,
                                    String catalogName,
                                    String schemaName,
                                    String tableName) throws SQLException {

        //fixme: не очень удачный подход. Лучше хранить отдельно для каждого типа БД
        if (metaData.storesUpperCaseIdentifiers()) {
            catalogName = StringUtils.upperCase(catalogName);
            schemaName = StringUtils.upperCase(schemaName);
            tableName = StringUtils.upperCase(tableName);
        } else if (metaData.storesLowerCaseIdentifiers()) {
            catalogName = StringUtils.lowerCase(catalogName);
            schemaName = StringUtils.lowerCase(schemaName);
            tableName = StringUtils.lowerCase(tableName);
        }

        return super.getRelDataType(metaData, catalogName, schemaName, tableName);
    }
}
