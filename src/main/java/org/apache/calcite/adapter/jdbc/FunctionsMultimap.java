package org.apache.calcite.adapter.jdbc;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.NotImplementedException;
import org.apache.drill.exec.store.idvp.jdbc.JdbcRecordReader;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author ozinoviev
 * @since 16.08.2019
 */
@SuppressWarnings("NullableProblems")
class FunctionsMultimap implements Multimap<String, Function> {

    private final JdbcSchema schema;
    private volatile Set<String> functionNames;
    private final ConcurrentHashMap<String, Collection<Function>> signatures = new ConcurrentHashMap<>();

    FunctionsMultimap(JdbcSchema schema) {
        this.schema = schema;
    }

    @Override
    public int size() {
        return keySet().size();
    }

    @Override
    public boolean isEmpty() {
        return size() != 0;
    }

    @Override
    public boolean containsKey(@Nullable Object key) {
        if (key == null) {
            return false;
        }

        //noinspection SuspiciousMethodCalls
        return keySet().contains(key);
    }


    @Override
    public Collection<Function> get(@Nullable String key) {
        if (key == null || !keySet().contains(key)) {
            return ImmutableSet.of();
        }

        return signatures.computeIfAbsent(key, this::loadFunctionSignatures);
    }

    @Override
    public Set<String> keySet() {
        if (functionNames == null) {
            synchronized (this) {
                if (functionNames == null) {
                    loadFunctionNames();
                }
            }
        }

        return functionNames;
    }

    @Override
    public Multiset<String> keys() {
        return HashMultiset.create(keySet());
    }

    @Override
    public void clear() {

    }

    private void loadFunctionNames() {
        ImmutableSet.Builder<String> namesBuilder = new ImmutableSet.Builder<>();

        try (Connection connection = schema.dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet rs = metaData.getFunctions(schema.catalog, schema.schema, null)) {
                while (rs.next()) {
                    if (rs.getInt("FUNCTION_TYPE") == DatabaseMetaData.functionReturnsTable) {
                        namesBuilder.add(rs.getString("FUNCTION_NAME"));
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Exception while reading functions", e);
        }

        functionNames = namesBuilder.build();
    }

    private Collection<Function> loadFunctionSignatures(String functionName) {
        try (Connection connection = schema.dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();

            ImmutableList.Builder<FunctionParameter> parametersBuilder = ImmutableList.builder();

            try (ResultSet rs = metaData.getFunctionColumns(schema.catalog, schema.schema, functionName, null)) {
                while (rs.next()) {
                    int columnType = rs.getInt("COLUMN_TYPE");
                    if (columnType != DatabaseMetaData.functionColumnIn && columnType != DatabaseMetaData.functionColumnResult) {
                        return Collections.emptySet();
                    }

                    SqlTypeName type = JdbcRecordReader.getNameForJdbcType(rs.getInt("DATA_TYPE"));

                    String name = rs.getString("COLUMN_NAME");
                    int ordinal = rs.getInt("ORDINAL_POSITION");
                    boolean nullable = rs.getInt("NULLABLE") != DatabaseMetaData.functionNoNulls;

                    Number precision = (Number) rs.getObject("PRECISION");
                    Number scale = (Number) rs.getObject("SCALE");

                    if (columnType == DatabaseMetaData.functionColumnIn) {
                        parametersBuilder.add(new JdbcFunctionParameter(name, type, ordinal, nullable, precision, scale));
                    }

                    //todo: дождаться Drill 17
                    //return Collections.singletonList(new WithOptionsTableMacro)
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Exception while reading functions", e);
        }

    }

    //region not supported
    // Методы не поддерживаемые в текущей реализации, так как они не успользуются в calcite

    @Override
    public Collection<Function> values() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<Map.Entry<String, Function>> entries() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Collection<Function>> asMap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsValue(@Nullable Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsEntry(@Nullable Object key, @Nullable Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean put(@Nullable String key, @Nullable Function value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(@Nullable Object key, @Nullable Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean putAll(@Nullable String key, Iterable<? extends Function> values) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean putAll(Multimap<? extends String, ? extends Function> multimap) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<Function> replaceValues(@Nullable String key, Iterable<? extends Function> values) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<Function> removeAll(@Nullable Object key) {
        throw new UnsupportedOperationException();
    }
    //endregion

    private static final class JdbcFunctionParameter implements FunctionParameter {


        private final String name;
        private final SqlTypeName type;
        private final int ordinal;
        @SuppressWarnings("FieldCanBeLocal")
        private final boolean nullable;
        private final Number precision;
        private final Number scale;

        JdbcFunctionParameter(String name,
                              SqlTypeName type,
                              int ordinal,
                              boolean nullable,
                              Number precision,
                              Number scale) {

            this.name = name;
            this.type = type;
            this.ordinal = ordinal;
            this.nullable = nullable;
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public int getOrdinal() {
            return ordinal;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public RelDataType getType(RelDataTypeFactory factory) {
            RelDataType type;
            if (precision != null && scale != null) {
                type = factory.createSqlType(this.type, precision.intValue(), scale.intValue());
            } else if (precision != null) {
                type = factory.createSqlType(this.type, precision.intValue());
            } else {
                type = factory.createSqlType(this.type);
            }

            return type;
        }

        @Override
        public boolean isOptional() {
            return false;
        }
    }
}
