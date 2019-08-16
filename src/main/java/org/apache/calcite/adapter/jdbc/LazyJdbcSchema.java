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

import com.google.common.collect.Multimap;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.exec.store.idvp.jdbc.JdbcRecordReader;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
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
    protected Multimap<String, Function> getFunctions() {

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

        //region copy of super.getRelDataType()

        final ResultSet resultSet =
                metaData.getColumns(catalogName, schemaName, tableName, null);

        // Temporary type factory, just for the duration of this method. Allowable
        // because we're creating a proto-type, not a type; before being used, the
        // proto-type will be copied into a real type factory.
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        while (resultSet.next()) {
            final String columnName = resultSet.getString(4);
            final int dataType = resultSet.getInt(5);
            final String typeString = resultSet.getString(6);
            final int precision;
            final int scale;
            switch (SqlType.valueOf(dataType)) {
                case TIMESTAMP:
                case TIME:
                    precision = resultSet.getInt(9); // SCALE
                    scale = 0;
                    break;
                default:
                    precision = resultSet.getInt(7); // SIZE
                    scale = resultSet.getInt(9); // SCALE
                    break;
            }
            RelDataType sqlType = sqlType(typeFactory, dataType, precision, scale, typeString);
            boolean nullable = resultSet.getInt(11) != DatabaseMetaData.columnNoNulls;
            fieldInfo.add(columnName, sqlType).nullable(nullable);
        }
        resultSet.close();
        return RelDataTypeImpl.proto(fieldInfo.build());
    }
    //endregion

    //region custom sql type
    private RelDataType sqlType(RelDataTypeFactory typeFactory, int dataType,
                                int precision, int scale, String typeString) {
        // Fall back to ANY if type is unknown
        final SqlTypeName sqlTypeName = Util.first(JdbcRecordReader.getNameForJdbcType(dataType), SqlTypeName.ANY);
        if (sqlTypeName == SqlTypeName.ARRAY) {
            RelDataType component = null;
            if (typeString != null && typeString.endsWith(" ARRAY")) {
                // E.g. hsqldb gives "INTEGER ARRAY", so we deduce the component type
                // "INTEGER".
                final String remaining = typeString.substring(0,
                        typeString.length() - " ARRAY".length());
                component = parseTypeString(typeFactory, remaining);
            }
            if (component == null) {
                component = typeFactory.createTypeWithNullability(
                        typeFactory.createSqlType(SqlTypeName.ANY), true);
            }
            return typeFactory.createArrayType(component, -1);
        }

        if (precision >= 0
                && scale >= 0
                && sqlTypeName.allowsPrecScale(true, true)) {
            return typeFactory.createSqlType(sqlTypeName, precision, scale);
        } else if (precision >= 0 && sqlTypeName.allowsPrecNoScale()) {
            return typeFactory.createSqlType(sqlTypeName, precision);
        } else {
            assert sqlTypeName.allowsNoPrecNoScale();
            return typeFactory.createSqlType(sqlTypeName);
        }
    }
    //endregion

    //region copy of parseTypeString
    /** Given "INTEGER", returns BasicSqlType(INTEGER).
     * Given "VARCHAR(10)", returns BasicSqlType(VARCHAR, 10).
     * Given "NUMERIC(10, 2)", returns BasicSqlType(NUMERIC, 10, 2). */
    private RelDataType parseTypeString(RelDataTypeFactory typeFactory,
                                        String typeString) {
        int precision = -1;
        int scale = -1;
        int open = typeString.indexOf("(");
        if (open >= 0) {
            int close = typeString.indexOf(")", open);
            if (close >= 0) {
                String rest = typeString.substring(open + 1, close);
                typeString = typeString.substring(0, open);
                int comma = rest.indexOf(",");
                if (comma >= 0) {
                    precision = Integer.parseInt(rest.substring(0, comma));
                    scale = Integer.parseInt(rest.substring(comma));
                } else {
                    precision = Integer.parseInt(rest);
                }
            }
        }
        try {
            final SqlTypeName typeName = SqlTypeName.valueOf(typeString);
            return typeName.allowsPrecScale(true, true)
                    ? typeFactory.createSqlType(typeName, precision, scale)
                    : typeName.allowsPrecScale(true, false)
                    ? typeFactory.createSqlType(typeName, precision)
                    : typeFactory.createSqlType(typeName);
        } catch (IllegalArgumentException e) {
            return typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.ANY), true);
        }
    }
    //endregion
}
