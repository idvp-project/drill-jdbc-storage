/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.idvp.jdbc;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.physical.PhysicalPlanCreator;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.visitor.PrelVisitor;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;

/**
 * Represents a JDBC Plan once the children nodes have been rewritten into SQL.
 */
public class JdbcPrel extends AbstractRelNode implements Prel {

    private final String sql;
    private final double rows;
    private final DrillJdbcConvention convention;

    JdbcPrel(RelOptCluster cluster, RelTraitSet traitSet, JdbcIntermediatePrel prel) {
        super(cluster, traitSet);
        final RelNode input = prel.getInput();
        //noinspection deprecation
        rows = input.estimateRowCount(cluster.getMetadataQuery());
        rowType = input.getRowType();
        convention = (DrillJdbcConvention) input.getTraitSet().getTrait(ConventionTraitDef.INSTANCE);

        // generate sql for tree.
        final SqlDialect dialect = convention.getPlugin().getDialect();
        final JdbcImplementor jdbcImplementor = new JdbcImplementor(
                dialect,
                (JavaTypeFactory) getCluster().getTypeFactory());
        final JdbcImplementor.Result result =
                jdbcImplementor.visitChild(0, input.accept(new SubsetRemover()));

        SqlPrettyWriter sqlWriter = new SqlPrettyWriter(dialect);
        sqlWriter.setSelectListItemsOnSeparateLines(false);
        sqlWriter.setQuoteAllIdentifiers(false);
        sqlWriter.setIndentation(0);

        SqlNode statement = result.asStatement();
        if (statement instanceof SqlSelect) {
            SqlSelect select = (SqlSelect) statement;
            select.setSelectList(rewriteSelectList(select.getSelectList(), rowType));
        }

        statement.unparse(sqlWriter, 0, 0);

        sql = sqlWriter.toString();
    }

    //Substitute newline. Also stripping away single line comments. Expecting hints to be nested in '/* <hint> */'
    private String stripToOneLineSql(String sql) {
        StringBuilder strippedSqlTextBldr = new StringBuilder(sql.length());
        String sqlToken[] = sql.split("\\n");
        for (String sqlTextLine : sqlToken) {
            if (!StringUtils.trimToEmpty(sqlTextLine).startsWith("--")) { //Skip comments
                strippedSqlTextBldr
                        .append(sqlTextLine)
                        .append(' ');
            }
        }

        return strippedSqlTextBldr.toString();
    }

    private SqlNodeList rewriteSelectList(SqlNodeList selectList, RelDataType rowType) {
        if (selectList == null) {
            return null;
        }

        Function<Integer, String> aliasGenerator;
        if (rowType.getFieldCount() == selectList.size()) {
            aliasGenerator = index -> rowType.getFieldNames().get(index);
        } else {
            aliasGenerator = index -> "EXPR$" + index;
        }

        return (SqlNodeList) selectList.accept(new AliasShuttle(aliasGenerator));
    }

    @Override
    public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) {
        JdbcGroupScan output = new JdbcGroupScan(sql, convention.getPlugin(), rows);
        return creator.addMetadata(this, output);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("sql", stripToOneLineSql(sql));
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return rows;
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Iterator<Prel> iterator() {
        return Collections.emptyIterator();
    }

    @Override
    public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
        return logicalVisitor.visitPrel(this, value);
    }

    @Override
    public SelectionVectorMode[] getSupportedEncodings() {
        return SelectionVectorMode.DEFAULT;
    }

    @Override
    public SelectionVectorMode getEncoding() {
        return SelectionVectorMode.NONE;
    }

    @Override
    public boolean needsFinalColumnReordering() {
        return false;
    }

    private class SubsetRemover extends RelShuttleImpl {

        @Override
        public RelNode visit(RelNode other) {
            if (other instanceof RelSubset) {
                return ((RelSubset) other).getBest().accept(this);
            } else {
                return super.visit(other);
            }
        }
    }

    private class AliasShuttle extends SqlShuttle {
        private int index = -1;
        private final Function<Integer, String> aliasGenerator;

        AliasShuttle(Function<Integer, String> aliasGenerator) {
            this.aliasGenerator = Preconditions.checkNotNull(aliasGenerator);
        }

        @Override
        public SqlNode visit(SqlCall call) {
            index++;
            if (call.getOperator() instanceof SqlAsOperator) {
                return call;
            }

            return wrapToAsNode(call);
        }

        @Override
        public SqlNode visit(SqlLiteral literal) {
            index++;
            return wrapToAsNode(literal);
        }

        @Override
        public SqlNode visit(SqlDynamicParam param) {
            index++;
            return wrapToAsNode(param);
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            index++;
            return id;
        }

        @Override
        public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
            index++;
            return wrapToAsNode(intervalQualifier);
        }

        @Override
        public SqlNode visit(SqlDataTypeSpec type) {
            index++;
            throw new IllegalStateException("SqlDataTypeSpec not supported");
        }

        private SqlNode wrapToAsNode(SqlNode node) {
            String fieldName = aliasGenerator.apply(index);
            SqlNode[] operands = {
                    node,
                    new SqlIdentifier(fieldName, node.getParserPosition())
            };

            return new SqlBasicCall(
                    SqlStdOperatorTable.AS,
                    operands,
                    node.getParserPosition()
            );
        }
    }

}
