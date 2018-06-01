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
package org.apache.calcite.sql;

import com.google.common.collect.ImmutableSortedMap;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.drill.common.exceptions.UserException;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author Oleg Zinoviev
 * @since 11.09.2017
 **/
@SuppressWarnings("SpellCheckingInspection")
public class JdbcSqlDialect extends SqlDialect {


    private final static ImmutableSortedMap<String, DatabaseProduct> DRIVERS_MAP = ImmutableSortedMap.<String, DatabaseProduct>orderedBy(String::compareToIgnoreCase)
            .put("com.simba.googlebigquery.jdbc42.Driver", DatabaseProduct.BIG_QUERY)
            .put("com.simba.googlebigquery.jdbc41.Driver", DatabaseProduct.BIG_QUERY)
            .put("com.microsoft.sqlserver.jdbc.SQLServerDriver", DatabaseProduct.MSSQL)
            .put("com.mysql.jdbc.Driver", DatabaseProduct.MYSQL)
            .put("oracle.jdbc.driver.OracleDriver", DatabaseProduct.ORACLE)
            .put("oracle.jdbc.OracleDriver", DatabaseProduct.ORACLE)
            .put("org.apache.derby.jdbc.EmbeddedDriver", DatabaseProduct.DERBY)
            .put("com.ibm.db2.jcc.DB2Driver", DatabaseProduct.DB2)
            .put("com.frontbase.jdbc.FBJDriver", DatabaseProduct.DB2)
            .put("org.firebirdsql.jdbc.FBDriver", DatabaseProduct.FIREBIRD)
            .put("org.h2.Driver", DatabaseProduct.H2)
            .put("org.apache.hive.jdbc.HiveDriver", DatabaseProduct.HIVE)
            .put("com.informix.jdbc.IfxDriver", DatabaseProduct.INFORMIX)
            .put("com.ingres.jdbc.IngresDriver", DatabaseProduct.INGRES)
            .put("org.luciddb.jdbc.LucidDbClientDriver", DatabaseProduct.LUCIDDB)
            .put("interbase.interclient.Driver", DatabaseProduct.INTERBASE)
            .put("org.apache.phoenix.jdbc.PhoenixDriver", DatabaseProduct.PHOENIX)
            .put("org.postgresql.Driver", DatabaseProduct.POSTGRESQL)
            .put("org.netezza.Driver", DatabaseProduct.NETEZZA)
            .put("com.hp.t4jdbc.HPT4Driver", DatabaseProduct.NEOVIEW)
            .put("com.sybase.jdbc4.jdbc.SybDriver", DatabaseProduct.SYBASE)
            .put("com.teradata.jdbc.TeraDriver", DatabaseProduct.TERADATA)
            .put("org.hsqldb.jdbcDriver", DatabaseProduct.HSQLDB)
            .put("com.vertica.Driver", DatabaseProduct.VERTICA)
            .put("com.amazon.redshift.jdbc4.Driver", DatabaseProduct.REDSHIFT)
            .build();

    public static JdbcSqlDialect createDialect(DatabaseMetaData databaseMetaData, DataSource dataSource) {
        SqlDialectFactory factory = new SqlDialectFactoryImpl();
        SqlDialect sqlDialect = factory.create(databaseMetaData);
        return new JdbcSqlDialect(sqlDialect, dataSource);
    }

    public static SqlDialect createByDriverName(String driver, DataSource dataSource) {
        DatabaseProduct product = DRIVERS_MAP.get(driver);
        if (product == null) {
            return new JdbcSqlDialect(AnsiSqlDialect.DEFAULT, dataSource);
        }

        return new JdbcSqlDialect(product.getDialect(), dataSource);
    }

    private final SqlDialect dialect;
    private final DataSource dataSource;

    private volatile SqlIdentifierValidator validator;

    private JdbcSqlDialect(SqlDialect dialect,
                           DataSource dataSource) {
        super(emptyContext()
                .withDatabaseProduct(DatabaseProduct.UNKNOWN)
                .withIdentifierQuoteString("`"));

        this.dialect = dialect;
        this.dataSource = dataSource;
    }

    @Override
    public boolean identifierNeedsToBeQuoted(String val) {
        if (validator == null) {
            synchronized (this) {
                if (validator == null) {
                    try {
                        validator = new SqlIdentifierValidator(dataSource);
                    } catch (SQLException | IOException e) {
                        throw UserException.planError(e)
                                .message("The JDBC storage plugin failed while trying configure SQL Dialect")
                                .build(LOGGER);
                    }
                }
            }
        }

        return validator.identifierNeedsToBeQuoted(val);
    }



    //region SqlDialect delegation
    @Override
    public String quoteStringLiteral(String val) {
        return dialect.quoteStringLiteral(val);
    }

    @Override
    public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        dialect.unparseCall(writer, call, leftPrec, rightPrec);
    }

    @Override
    public void unparseDateTimeLiteral(SqlWriter writer, SqlAbstractDateTimeLiteral literal, int leftPrec, int rightPrec) {
        dialect.unparseDateTimeLiteral(writer, literal, leftPrec, rightPrec);
    }

    @Override
    public void quoteStringLiteralUnicode(StringBuilder buf, String val) {
        dialect.quoteStringLiteralUnicode(buf, val);
    }

    @Override
    public String unquoteStringLiteral(String val) {
        return dialect.unquoteStringLiteral(val);
    }

    @Override
    public boolean allowsAs() {
        return dialect.allowsAs();
    }

    @Override
    public boolean requiresAliasForFromItems() {
        return dialect.requiresAliasForFromItems();
    }

    @Override
    public boolean hasImplicitTableAlias() {
        return dialect.hasImplicitTableAlias();
    }

    @Override
    public String quoteTimestampLiteral(Timestamp timestamp) {
        return dialect.quoteTimestampLiteral(timestamp);
    }

    @SuppressWarnings("deprecation")
    @Override
    @Deprecated
    public DatabaseProduct getDatabaseProduct() {
        return dialect.getDatabaseProduct();
    }

    @Override
    public boolean supportsCharSet() {
        return dialect.supportsCharSet();
    }

    @Override
    public boolean supportsAggregateFunction(SqlKind kind) {
        switch (kind) {
            case COUNT:
            case SUM:
            //case SUM0:
            case AVG:
            case MIN:
            case MAX:
                return true;
        }
        return false;
        //return dialect.supportsAggregateFunction(kind);
    }

    @Override
    public CalendarPolicy getCalendarPolicy() {
        return dialect.getCalendarPolicy();
    }

    @Override
    public SqlNode getCastSpec(RelDataType type) {
        return dialect.getCastSpec(type);
    }

    @Override
    public SqlNode rewriteSingleValueExpr(SqlNode aggCall) {
        return dialect.rewriteSingleValueExpr(aggCall);
    }

    @Override
    public SqlNode emulateNullDirection(SqlNode node, boolean nullsFirst, boolean desc) {
        return dialect.emulateNullDirection(node, nullsFirst, desc);
    }

    @Override
    public SqlNode emulateNullDirectionWithIsNull(SqlNode node, boolean nullsFirst, boolean desc) {
        return dialect.emulateNullDirectionWithIsNull(node, nullsFirst, desc);
    }

    @Override
    public boolean supportsOffsetFetch() {
        return dialect.supportsOffsetFetch();
    }

    @Override
    public boolean supportsNestedAggregations() {
        return dialect.supportsNestedAggregations();
    }

    @Override
    public NullCollation getNullCollation() {
        return dialect.getNullCollation();
    }

    @Override
    public RelFieldCollation.NullDirection defaultNullDirection(RelFieldCollation.Direction direction) {
        return dialect.defaultNullDirection(direction);
    }

    @Override
    public boolean supportsAliasedValues() {
        return dialect.supportsAliasedValues();
    }

    @Override
    public String quoteIdentifier(String val) {
        return dialect.quoteIdentifier(val);
    }

    @Override
    public StringBuilder quoteIdentifier(StringBuilder buf, String val) {
        return dialect.quoteIdentifier(buf, val);
    }

    @Override
    public StringBuilder quoteIdentifier(StringBuilder buf, List<String> identifiers) {
        return dialect.quoteIdentifier(buf, identifiers);
    }
    //region SqlDialect delegation
}
