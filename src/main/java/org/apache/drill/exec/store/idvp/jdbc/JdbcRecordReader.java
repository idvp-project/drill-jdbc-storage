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
import com.google.common.collect.ImmutableMap;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.idvp.jdbc.copiers.*;
import org.apache.drill.exec.vector.*;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.*;

@SuppressWarnings("unchecked")
public class JdbcRecordReader extends AbstractRecordReader {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
            .getLogger(JdbcRecordReader.class);

    private static final ImmutableMap<Integer, MinorType> JDBC_TYPE_MAPPINGS;
    private final DataSource source;
    private ResultSet resultSet;
    private final String storagePluginName;
    private Connection connection;
    private Statement statement;
    private final String sql;
    private ImmutableList<ValueVector> vectors;
    private ImmutableList<Copier<?>> copiers;

    JdbcRecordReader(DataSource source, String sql, String storagePluginName) {
        this.source = source;
        this.sql = sql;
        this.storagePluginName = storagePluginName;
    }

    static {
        JDBC_TYPE_MAPPINGS = (ImmutableMap<Integer, MinorType>) (Object) ImmutableMap.builder()
                .put(java.sql.Types.DOUBLE, MinorType.FLOAT8)
                .put(java.sql.Types.FLOAT, MinorType.FLOAT4)
                .put(java.sql.Types.TINYINT, MinorType.INT)
                .put(java.sql.Types.SMALLINT, MinorType.INT)
                .put(java.sql.Types.INTEGER, MinorType.INT)
                .put(java.sql.Types.BIGINT, MinorType.BIGINT)

                .put(java.sql.Types.CHAR, MinorType.VARCHAR)
                .put(java.sql.Types.VARCHAR, MinorType.VARCHAR)
                .put(java.sql.Types.LONGVARCHAR, MinorType.VARCHAR)
                .put(java.sql.Types.CLOB, MinorType.VARCHAR)

                .put(java.sql.Types.NCHAR, MinorType.VARCHAR)
                .put(java.sql.Types.NVARCHAR, MinorType.VARCHAR)
                .put(java.sql.Types.LONGNVARCHAR, MinorType.VARCHAR)

                .put(java.sql.Types.VARBINARY, MinorType.VARBINARY)
                .put(java.sql.Types.LONGVARBINARY, MinorType.VARBINARY)
                .put(java.sql.Types.BLOB, MinorType.VARBINARY)

                .put(java.sql.Types.NUMERIC, MinorType.FLOAT8)
                .put(java.sql.Types.DECIMAL, MinorType.FLOAT8)
                .put(java.sql.Types.REAL, MinorType.FLOAT8)

                .put(java.sql.Types.DATE, MinorType.DATE)
                .put(java.sql.Types.TIME, MinorType.TIME)
                .put(java.sql.Types.TIMESTAMP, MinorType.TIMESTAMP)

                .put(java.sql.Types.BOOLEAN, MinorType.BIT)

                .put(java.sql.Types.BIT, MinorType.BIT)

                .build();
    }

    private static String nameFromType(int javaSqlType) {
        try {
            for (Field f : java.sql.Types.class.getFields()) {
                if (java.lang.reflect.Modifier.isStatic(f.getModifiers()) &&
                        f.getType() == int.class &&
                        f.getInt(null) == javaSqlType) {
                    return f.getName();

                }
            }
        } catch (IllegalArgumentException | IllegalAccessException e) {
            logger.error("nameFromType", e);
        }

        return Integer.toString(javaSqlType);

    }

    private Copier<?> getCopier(int offset, ResultSet result, ValueVector v) {

        if (v instanceof NullableBigIntVector) {
            return new BigIntCopier(offset, result, (NullableBigIntVector.Mutator) v.getMutator());
        } else if (v instanceof NullableFloat4Vector) {
            return new Float4Copier(offset, result, (NullableFloat4Vector.Mutator) v.getMutator());
        } else if (v instanceof NullableFloat8Vector) {
            return new Float8Copier(offset, result, (NullableFloat8Vector.Mutator) v.getMutator());
        } else if (v instanceof NullableIntVector) {
            return new IntCopier(offset, result, (NullableIntVector.Mutator) v.getMutator());
        } else if (v instanceof NullableVarCharVector) {
            return new VarCharCopier(resultSet, offset, result, (NullableVarCharVector.Mutator) v.getMutator());
        } else if (v instanceof NullableVarBinaryVector) {
            return new VarBinaryCopier(offset, result, (NullableVarBinaryVector.Mutator) v.getMutator());
        } else if (v instanceof NullableDateVector) {
            return new DateCopier(offset, result, (NullableDateVector.Mutator) v.getMutator());
        } else if (v instanceof NullableTimeVector) {
            return new TimeCopier(offset, result, (NullableTimeVector.Mutator) v.getMutator());
        } else if (v instanceof NullableTimeStampVector) {
            return new TimeStampCopier(offset, result, (NullableTimeStampVector.Mutator) v.getMutator());
        } else if (v instanceof NullableBitVector) {
            return new BitCopier(offset, result, (NullableBitVector.Mutator) v.getMutator());
        }

        throw new IllegalArgumentException("Unknown how to handle vector.");
    }

    @Override
    public void setup(OperatorContext operatorContext, OutputMutator output) throws ExecutionSetupException {
        try {

            connection = source.getConnection();
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);

            final ResultSetMetaData meta = resultSet.getMetaData();
            final int columns = meta.getColumnCount();
            ImmutableList.Builder<ValueVector> vectorBuilder = ImmutableList.builder();
            ImmutableList.Builder<Copier<?>> copierBuilder = ImmutableList.builder();

            for (int i = 1; i <= columns; i++) {
                final String name = meta.getColumnLabel(i);
                final int jdbcType = meta.getColumnType(i);
                MinorType minorType = JDBC_TYPE_MAPPINGS.get(jdbcType);
                if (minorType == null) {

                    logger.warn("Ignoring column that is unsupported.", UserException
                            .unsupportedError()
                            .message(
                                    "A column you queried has a data type that is not currently supported by the JDBC storage plugin. "
                                            + "The column's name was %s and its JDBC data type was %s. ",
                                    name,
                                    nameFromType(jdbcType))
                            .addContext("sql", sql)
                            .addContext("column Name", name)
                            .addContext("plugin", storagePluginName)
                            .build(logger));

                    continue;
                }

                final MajorType type = Types.optional(minorType);
                final MaterializedField field = MaterializedField.create(name, type);
                final Class<? extends ValueVector> clazz = TypeHelper.getValueVectorClass(minorType, type.getMode());
                ValueVector vector = output.addField(field, clazz);
                vectorBuilder.add(vector);
                copierBuilder.add(getCopier(i, resultSet, vector));

            }

            vectors = vectorBuilder.build();
            copiers = copierBuilder.build();

        } catch (SQLException | SchemaChangeException e) {
            throw UserException.dataReadError(e)
                    .message("The JDBC storage plugin failed while trying setup the SQL query. ")
                    .addContext("sql", sql)
                    .addContext("plugin", storagePluginName)
                    .build(logger);
        }
    }


    @Override
    public int next() {
        int counter = 0;
        try {
            while (counter < 4095) { // loop at 4095 since nullables use one more than record count and we
                // allocate on powers of two.
                if (!resultSet.next()) {
                    break;
                }

                for (Copier<?> c : copiers) {
                    c.copy(counter);
                }
                counter++;
            }
        } catch (SQLException e) {
            throw UserException
                    .dataReadError(e)
                    .message("Failure while attempting to read from database.")
                    .addContext("sql", sql)
                    .addContext("plugin", storagePluginName)
                    .build(logger);
        }

        for (ValueVector vv : vectors) {
            vv.getMutator().setValueCount(counter > 0 ? counter : 0);
        }

        return counter > 0 ? counter : 0;
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(resultSet, statement, connection);
    }


}
