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

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.PublicJdbcTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDialect;

import javax.sql.DataSource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleJdbcSchema extends JdbcSchema {

    private final Map<String, Table> tables = new ConcurrentHashMap<>();
    private final String catalog;
    private final String schema;

    SimpleJdbcSchema(DataSource dataSource, SqlDialect dialect, JdbcConvention convention, String catalog, String schema) {
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

        return new PublicJdbcTable(this, catalog, schema, name, TableType.TABLE);
    }

}
