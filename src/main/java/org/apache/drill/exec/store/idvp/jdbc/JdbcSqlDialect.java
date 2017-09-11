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

import org.apache.calcite.sql.SqlDialect;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.regex.Pattern;

/**
 * @author Oleg Zinoviev
 * @since 11.09.2017
 **/
public class JdbcSqlDialect extends SqlDialect {

    public static JdbcSqlDialect create(DatabaseMetaData databaseMetaData) {
        String identifierQuoteString;
        try {
            identifierQuoteString = databaseMetaData.getIdentifierQuoteString();
        } catch (SQLException var5) {
            throw SqlDialect.FakeUtil.newInternal(var5, "while quoting identifier");
        }

        String databaseProductName;
        try {
            databaseProductName = databaseMetaData.getDatabaseProductName();
        } catch (SQLException var4) {
            throw SqlDialect.FakeUtil.newInternal(var4, "while detecting database product");
        }

        SqlDialect.DatabaseProduct databaseProduct = getProduct(databaseProductName, null);
        return new JdbcSqlDialect(databaseProduct, databaseProductName, identifierQuoteString);
    }

    private JdbcSqlDialect(DatabaseProduct databaseProduct,
                           String databaseProductName,
                           String identifierQuoteString) {
        super(databaseProduct, databaseProductName, identifierQuoteString);
    }

    @Override
    public boolean identifierNeedsToBeQuoted(String val) {
        return !Pattern.compile("^[A-Za-z_0-9]+").matcher(val).matches();
    }
}
