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

import com.google.common.base.Charsets;
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
import org.apache.drill.exec.expr.holders.Decimal18Holder;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.server.options.TypeValidators;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.vector.*;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;

@SuppressWarnings("unchecked")
class JdbcRecordReader extends AbstractRecordReader {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JdbcRecordReader.class);
    private static final TypeValidators.BooleanValidator DECIMAL_ENABLED = new TypeValidators.BooleanValidator("planner.enable_decimal_data_type", false);

    private static final ImmutableMap<Integer, TypeInfo> JDBC_TYPE_MAPPINGS;

    static {
        JDBC_TYPE_MAPPINGS = ImmutableMap.<Integer, TypeInfo>builder()
                .put(java.sql.Types.TINYINT, new TypeInfo(MinorType.INT))
                .put(java.sql.Types.SMALLINT, new TypeInfo(MinorType.INT))
                .put(java.sql.Types.INTEGER, new TypeInfo(MinorType.INT))
                .put(java.sql.Types.BIGINT, new TypeInfo(MinorType.BIGINT))

                .put(java.sql.Types.CHAR, new TypeInfo(MinorType.VARCHAR))
                .put(java.sql.Types.VARCHAR, new TypeInfo(MinorType.VARCHAR))
                .put(java.sql.Types.LONGVARCHAR, new TypeInfo(MinorType.VARCHAR))
                .put(java.sql.Types.CLOB, new TypeInfo(MinorType.VARCHAR))

                .put(java.sql.Types.NCHAR, new TypeInfo(MinorType.VARCHAR))
                .put(java.sql.Types.NVARCHAR, new TypeInfo(MinorType.VARCHAR))
                .put(java.sql.Types.LONGNVARCHAR, new TypeInfo(MinorType.VARCHAR))

                .put(java.sql.Types.VARBINARY, new TypeInfo(MinorType.VARBINARY))
                .put(java.sql.Types.LONGVARBINARY, new TypeInfo(MinorType.VARBINARY))
                .put(java.sql.Types.BLOB, new TypeInfo(MinorType.VARBINARY))

                .put(java.sql.Types.NUMERIC, new TypeInfo(MinorType.DECIMAL18))
                .put(java.sql.Types.DECIMAL, new TypeInfo(MinorType.DECIMAL18))
                .put(java.sql.Types.REAL, new TypeInfo(MinorType.FLOAT8))
                .put(java.sql.Types.DOUBLE, new TypeInfo(MinorType.FLOAT8))
                .put(java.sql.Types.FLOAT, new TypeInfo(MinorType.FLOAT4))

                .put(java.sql.Types.DATE, new TypeInfo(MinorType.DATE))
                .put(java.sql.Types.TIME, new TypeInfo(MinorType.TIME))
                .put(java.sql.Types.TIMESTAMP, new TypeInfo(MinorType.TIMESTAMP))

                .put(java.sql.Types.BOOLEAN, new TypeInfo(MinorType.BIT))

                .put(java.sql.Types.BIT, new TypeInfo(MinorType.BIT))

                // Потенциальный источник проблем
                .put(java.sql.Types.OTHER, new TypeInfo(MinorType.VARCHAR, new Object2VarCharCopier.Provider()))
                .put(java.sql.Types.ARRAY, new TypeInfo(MinorType.VARCHAR, new Object2VarCharCopier.Provider()))
                .put(java.sql.Types.STRUCT, new TypeInfo(MinorType.VARCHAR, new Object2VarCharCopier.Provider()))
                .put(java.sql.Types.JAVA_OBJECT, new TypeInfo(MinorType.VARCHAR, new Object2VarCharCopier.Provider()))
                .put(java.sql.Types.REF, new TypeInfo(MinorType.VARCHAR, new Object2VarCharCopier.Provider()))
                .put(java.sql.Types.DATALINK, new TypeInfo(MinorType.VARCHAR, new Object2VarCharCopier.Provider()))
                .put(java.sql.Types.NULL, new TypeInfo(MinorType.VARCHAR, new Object2VarCharCopier.Provider()))
                .put(java.sql.Types.SQLXML, new TypeInfo(MinorType.VARCHAR, new Object2VarCharCopier.Provider()))
                .put(java.sql.Types.ROWID, new TypeInfo(MinorType.VARCHAR, new Object2VarCharCopier.Provider()))
                .put(java.sql.Types.DISTINCT, new TypeInfo(MinorType.VARCHAR, new Object2VarCharCopier.Provider()))

                .build();
    }

    private final DataSource source;
    private final String storagePluginName;
    private final String sql;
    private final boolean decimalEnabled;
    private ResultSet resultSet;
    private Connection connection;
    private Statement statement;
    private ImmutableList<ValueVector> vectors;
    private ImmutableList<Copier<?>> copiers;

    JdbcRecordReader(FragmentContext context, DataSource source, String sql, String storagePluginName) {
        this.source = source;
        this.sql = sql;
        this.storagePluginName = storagePluginName;
        this.decimalEnabled = context.getOptions().getOption(DECIMAL_ENABLED);
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

    private Copier<?> getCopier(int offset, ResultSet result, TypeInfo typeInfo, ValueVector v) {

        if (typeInfo.copierOverride != null) {
            return typeInfo.copierOverride.create(offset, resultSet, v.getMutator());
        }

        if (v instanceof NullableBigIntVector) {
            return new BigIntCopier(offset, result, (NullableBigIntVector.Mutator) v.getMutator());
        } else if (v instanceof NullableFloat4Vector) {
            return new Float4Copier(offset, result, (NullableFloat4Vector.Mutator) v.getMutator());
        } else if (v instanceof NullableFloat8Vector) {
            return new Float8Copier(offset, result, (NullableFloat8Vector.Mutator) v.getMutator());
        } else if (v instanceof NullableIntVector) {
            return new IntCopier(offset, result, (NullableIntVector.Mutator) v.getMutator());
        } else if (v instanceof NullableVarCharVector) {
            return new VarCharCopier(offset, result, (NullableVarCharVector.Mutator) v.getMutator());
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
        } else if (v instanceof NullableDecimal18Vector) {
            return new DecimalCopier(offset, result, (NullableDecimal18Vector.Mutator) v.getMutator());
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
            Set<String> columnNames = new HashSet<>();
            ImmutableList.Builder<ValueVector> vectorBuilder = ImmutableList.builder();
            ImmutableList.Builder<Copier<?>> copierBuilder = ImmutableList.builder();

            for (int i = 1; i <= columns; i++) {
                String name = meta.getColumnLabel(i);

                final String baseName = name;
                int nameIndex = 0;
                while (columnNames.contains(name)) {
                    name = baseName + nameIndex++;
                }
                columnNames.add(name);

                final int jdbcType = meta.getColumnType(i);
                TypeInfo typeInfo = JDBC_TYPE_MAPPINGS.get(jdbcType);
                if (typeInfo == null) {
                    throw UserException
                            .unsupportedError()
                            .message(
                                    "A column you queried has a data type that is not currently supported by the JDBC storage plugin. "
                                            + "The column's name was %s and its JDBC data type was %s. ",
                                    name,
                                    nameFromType(jdbcType))
                            .addContext("sql", sql)
                            .addContext("column Name", name)
                            .addContext("plugin", storagePluginName)
                            .build(logger);
                }

                if (!decimalEnabled && typeInfo.minorType == MinorType.DECIMAL18) {
                    typeInfo = new TypeInfo(MinorType.FLOAT8);
                }

                final MajorType type = Types.optional(typeInfo.minorType);
                final MaterializedField field = MaterializedField.create(name, type);
                final Class<? extends ValueVector> clazz = TypeHelper.getValueVectorClass(typeInfo.minorType, type.getMode());
                ValueVector vector = output.addField(field, clazz);
                vectorBuilder.add(vector);
                copierBuilder.add(getCopier(i, resultSet, typeInfo, vector));

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

    private abstract static class Copier<T extends ValueVector.Mutator> {
        final int columnIndex;
        final ResultSet result;
        final T mutator;

        Copier(int columnIndex, ResultSet result, T mutator) {
            super();
            this.columnIndex = columnIndex;
            this.result = result;
            this.mutator = mutator;
        }

        abstract void copy(int index) throws SQLException;
    }

    private static class IntCopier extends Copier<NullableIntVector.Mutator> {
        IntCopier(int offset, ResultSet set, NullableIntVector.Mutator mutator) {
            super(offset, set, mutator);
        }

        @Override
        void copy(int index) throws SQLException {
            mutator.setSafe(index, result.getInt(columnIndex));
            if (result.wasNull()) {
                mutator.setNull(index);
            }
        }
    }

    private static class BigIntCopier extends Copier<NullableBigIntVector.Mutator> {
        BigIntCopier(int offset, ResultSet set, NullableBigIntVector.Mutator mutator) {
            super(offset, set, mutator);
        }

        @Override
        void copy(int index) throws SQLException {
            mutator.setSafe(index, result.getLong(columnIndex));
            if (result.wasNull()) {
                mutator.setNull(index);
            }
        }

    }

    private static class Float4Copier extends Copier<NullableFloat4Vector.Mutator> {

        Float4Copier(int columnIndex, ResultSet result, NullableFloat4Vector.Mutator mutator) {
            super(columnIndex, result, mutator);
        }

        @Override
        void copy(int index) throws SQLException {
            mutator.setSafe(index, result.getFloat(columnIndex));
            if (result.wasNull()) {
                mutator.setNull(index);
            }
        }

    }


    private static class Float8Copier extends Copier<NullableFloat8Vector.Mutator> {

        Float8Copier(int columnIndex, ResultSet result, NullableFloat8Vector.Mutator mutator) {
            super(columnIndex, result, mutator);
        }

        @Override
        void copy(int index) throws SQLException {
            mutator.setSafe(index, result.getDouble(columnIndex));
            if (result.wasNull()) {
                mutator.setNull(index);
            }

        }

    }

    private static class DecimalCopier extends Copier<NullableDecimal18Vector.Mutator> {

        DecimalCopier(int columnIndex, ResultSet result, NullableDecimal18Vector.Mutator mutator) {
            super(columnIndex, result, mutator);
        }

        @Override
        void copy(int index) throws SQLException {
            BigDecimal decimal = result.getBigDecimal(columnIndex);
            if (decimal != null) {
                Decimal18Holder h = new Decimal18Holder();
                h.precision = decimal.precision();
                h.scale = decimal.scale();
                h.value = DecimalUtility.getDecimal18FromBigDecimal(decimal, decimal.scale(), decimal.precision());
                mutator.setSafe(index, h);
            }
        }

    }

    private static class VarCharCopier extends Copier<NullableVarCharVector.Mutator> {

        VarCharCopier(int columnIndex, ResultSet result, NullableVarCharVector.Mutator mutator) {
            super(columnIndex, result, mutator);
        }

        @Override
        void copy(int index) throws SQLException {
            String val = result.getString(columnIndex);
            if (val != null) {
                byte[] record = val.getBytes(Charsets.UTF_8);
                mutator.setSafe(index, record, 0, record.length);
            }
        }

    }

    private static class Object2VarCharCopier extends Copier<NullableVarCharVector.Mutator> {

        final static class Provider implements TypeInfo.CopierProvider {

            @Override
            public Copier<?> create(int index, ResultSet resultSet, ValueVector.Mutator mutator) {
                return new Object2VarCharCopier(index, resultSet, (NullableVarCharVector.Mutator) mutator);
            }
        }


        Object2VarCharCopier(int columnIndex, ResultSet result, NullableVarCharVector.Mutator mutator) {
            super(columnIndex, result, mutator);
        }

        @Override
        void copy(int index) throws SQLException {
            Object object = result.getObject(columnIndex);
            String val = Objects.toString(object, null);
            if (object instanceof Array) {
                ((Array) object).free();
            }
            if (object instanceof SQLXML) {
                ((SQLXML) object).free();
            }
            if (val != null) {
                byte[] record = val.getBytes(Charsets.UTF_8);
                mutator.setSafe(index, record, 0, record.length);
            }
        }
    }

    private static class VarBinaryCopier extends Copier<NullableVarBinaryVector.Mutator> {

        VarBinaryCopier(int columnIndex, ResultSet result, NullableVarBinaryVector.Mutator mutator) {
            super(columnIndex, result, mutator);
        }

        @Override
        void copy(int index) throws SQLException {
            byte[] record = result.getBytes(columnIndex);
            if (record != null) {
                mutator.setSafe(index, record, 0, record.length);
            }
        }

    }

    private static class DateCopier extends Copier<NullableDateVector.Mutator> {

        private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

        DateCopier(int columnIndex, ResultSet result, NullableDateVector.Mutator mutator) {
            super(columnIndex, result, mutator);
        }

        @Override
        void copy(int index) throws SQLException {
            Date date = result.getDate(columnIndex, calendar);
            if (date != null) {
                mutator.setSafe(index, date.getTime());
            }
        }

    }

    private static class TimeCopier extends Copier<NullableTimeVector.Mutator> {

        private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

        TimeCopier(int columnIndex, ResultSet result, NullableTimeVector.Mutator mutator) {
            super(columnIndex, result, mutator);
        }

        @Override
        void copy(int index) throws SQLException {
            Time time = result.getTime(columnIndex, calendar);
            if (time != null) {
                mutator.setSafe(index, (int) time.getTime());
            }

        }

    }


    private static class TimeStampCopier extends Copier<NullableTimeStampVector.Mutator> {

        private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

        TimeStampCopier(int columnIndex, ResultSet result, NullableTimeStampVector.Mutator mutator) {
            super(columnIndex, result, mutator);
        }

        @Override
        void copy(int index) throws SQLException {
            Timestamp stamp = result.getTimestamp(columnIndex, calendar);
            if (stamp != null) {
                mutator.setSafe(index, stamp.getTime());
            }

        }

    }

    private static class BitCopier extends Copier<NullableBitVector.Mutator> {

        BitCopier(int columnIndex, ResultSet result, NullableBitVector.Mutator mutator) {
            super(columnIndex, result, mutator);
        }

        @Override
        void copy(int index) throws SQLException {
            mutator.setSafe(index, result.getBoolean(columnIndex) ? 1 : 0);
            if (result.wasNull()) {
                mutator.setNull(index);
            }
        }
    }

    private final static class TypeInfo {
        private final MinorType minorType;
        private final CopierProvider copierOverride;

        private TypeInfo(MinorType minorType, CopierProvider copierOverride) {
            this.minorType = minorType;
            this.copierOverride = copierOverride;
        }

        private TypeInfo(MinorType minorType) {
            this(minorType, null);
        }

        interface CopierProvider {
            Copier<?> create(int index, ResultSet resultSet, ValueVector.Mutator mutator);
        }
    }

}
