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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.drill.common.logical.StoragePluginConfig;

import java.io.IOException;
import java.util.Objects;

@SuppressWarnings("WeakerAccess")
@JsonTypeName(JdbcStorageConfig.NAME)
public class JdbcStorageConfig extends StoragePluginConfig {

    static final String NAME = "jdbc-idvp";
    private static final int DEFAULT_POOL_SIZE = 64;
    private static final int DEFAULT_VALIDATION_TIMEOUT = 500;
    private static final boolean DEFAULT_USE_STANDARD_DIALECT = false;
    private static final boolean DEFAULT_USE_EXTENDED_AGGREGATE_PUSH_DOWN = false;
    private static final int DEFAULT_EVICTION_PERIOD = 20000;
    private static final int DEFAULT_EVICTION_TIMEOUT = 30000;

    private final String driver;
    private final String url;
    private final String username;
    private final String password;

    private final int connectionPoolSize;
    private final int connectionEvictionTimeout;
    private final int connectionEvictionPeriod;
    private final int connectionValidationTimeout;

    private final boolean useStandardDialect;
    private final boolean useExtendedAggregatePushDown;

    //Конструктор для Jackson mapper. Создает объект со значениями свойств по-умолчанию
    @SuppressWarnings("unused")
    public JdbcStorageConfig() {
        this(null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    @JsonCreator
    public JdbcStorageConfig(
            @JsonProperty("driver") String driver,
            @JsonProperty("url") String url,
            @JsonProperty("username") String username,
            @JsonProperty("password") String password,
            @JsonProperty("connectionPoolSize") Integer connectionPoolSize,
            @JsonProperty("connectionEvictionTimeout") Integer connectionEvictionTimeout,
            @JsonProperty("connectionEvictionPeriod") Integer connectionEvictionPeriod,
            @JsonProperty("connectionValidationTimeout") Integer connectionValidationTimeout,
            @JsonProperty("useStandardDialect") Boolean useStandardDialect,
            @JsonProperty("useExtendedAggregatePushDown") Boolean useExtendedAggregatePushDown) {
        super();
        this.driver = driver;
        this.url = url;
        this.username = username;
        this.password = password;
        this.connectionPoolSize = ObjectUtils.firstNonNull(connectionPoolSize, DEFAULT_POOL_SIZE);
        this.connectionValidationTimeout = ObjectUtils.firstNonNull(connectionValidationTimeout, DEFAULT_VALIDATION_TIMEOUT);
        this.useStandardDialect = ObjectUtils.firstNonNull(useStandardDialect, DEFAULT_USE_STANDARD_DIALECT);
        this.useExtendedAggregatePushDown = ObjectUtils.firstNonNull(useExtendedAggregatePushDown, DEFAULT_USE_EXTENDED_AGGREGATE_PUSH_DOWN);
        this.connectionEvictionTimeout = ObjectUtils.firstNonNull(connectionEvictionTimeout, DEFAULT_EVICTION_TIMEOUT);
        this.connectionEvictionPeriod = ObjectUtils.firstNonNull(connectionEvictionPeriod, DEFAULT_EVICTION_PERIOD);
    }

    @JsonProperty
    public String getDriver() {
        return driver;
    }

    @JsonProperty
    public String getUrl() {
        return url;
    }

    @JsonProperty
    public String getUsername() {
        return username;
    }

    @JsonProperty
    public String getPassword() {
        return password;
    }

    @JsonProperty
    @JsonSerialize(using = ConnectionPoolSizeSerializer.class)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public int getConnectionPoolSize() {
        return connectionPoolSize;
    }

    @JsonProperty
    @JsonSerialize(using = ConnectionEvictionTimeoutSerializer.class)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public int getConnectionEvictionTimeout() {
        return connectionEvictionTimeout;
    }

    @JsonProperty
    @JsonSerialize(using = ConnectionEvictionPeriodSerializer.class)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public int getConnectionEvictionPeriod() {
        return connectionEvictionPeriod;
    }

    @JsonProperty
    @JsonSerialize(using = ConnectionValidationTimeoutSerializer.class)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public int getConnectionValidationTimeout() {
        return connectionValidationTimeout;
    }

    @JsonProperty
    @JsonSerialize(using = UseStandardDialectSerializer.class)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public boolean isUseStandardDialect() {
        return useStandardDialect;
    }

    @JsonProperty
    @JsonSerialize(using = UseExtendedAggregatePushDownSerializer.class)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public boolean isUseExtendedAggregatePushDown() {
        return useExtendedAggregatePushDown;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JdbcStorageConfig that = (JdbcStorageConfig) o;
        return connectionPoolSize == that.connectionPoolSize &&
                connectionEvictionTimeout == that.connectionEvictionTimeout &&
                connectionEvictionPeriod == that.connectionEvictionPeriod &&
                connectionValidationTimeout == that.connectionValidationTimeout &&
                useStandardDialect == that.useStandardDialect &&
                Objects.equals(driver, that.driver) &&
                Objects.equals(url, that.url) &&
                Objects.equals(username, that.username) &&
                Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(driver,
                url,
                username,
                password,
                connectionPoolSize,
                connectionEvictionTimeout,
                connectionEvictionPeriod,
                connectionValidationTimeout,
                useStandardDialect);
    }

    private static abstract class IntDefaultsSerializer extends StdSerializer<Integer> {

        private final int defaultValue;

        IntDefaultsSerializer(int defaultValue) {
            super(Integer.class);
            this.defaultValue = defaultValue;
        }

        @Override
        public boolean isEmpty(SerializerProvider provider, Integer value) {
            return super.isEmpty(provider, value) || value == null || value == defaultValue;
        }

        @Override
        public void serialize(Integer integer,
                              JsonGenerator jsonGenerator,
                              SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeNumber(integer);
        }
    }

    private static abstract class BooleanDefaultsSerializer extends StdSerializer<Boolean> {

        private final boolean defaultValue;

        BooleanDefaultsSerializer(boolean defaultValue) {
            super(Boolean.class);
            this.defaultValue = defaultValue;
        }

        @Override
        public boolean isEmpty(SerializerProvider provider, Boolean value) {
            return super.isEmpty(provider, value) || value == null || value == defaultValue;
        }

        @Override
        public void serialize(Boolean bool,
                              JsonGenerator jsonGenerator,
                              SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeBoolean(bool);
        }
    }

    private final static class ConnectionPoolSizeSerializer extends IntDefaultsSerializer {
        public ConnectionPoolSizeSerializer() {
            super(DEFAULT_POOL_SIZE);
        }
    }

    private final static class ConnectionEvictionTimeoutSerializer extends IntDefaultsSerializer {
        public ConnectionEvictionTimeoutSerializer() {
            super(DEFAULT_EVICTION_TIMEOUT);
        }
    }

    private final static class ConnectionEvictionPeriodSerializer extends IntDefaultsSerializer {
        public ConnectionEvictionPeriodSerializer() {
            super(DEFAULT_EVICTION_PERIOD);
        }
    }

    private final static class ConnectionValidationTimeoutSerializer extends IntDefaultsSerializer {
        public ConnectionValidationTimeoutSerializer() {
            super(DEFAULT_VALIDATION_TIMEOUT);
        }
    }

    private final static class UseStandardDialectSerializer extends BooleanDefaultsSerializer {
        public UseStandardDialectSerializer() {
            super(DEFAULT_USE_STANDARD_DIALECT);
        }
    }

    private final static class UseExtendedAggregatePushDownSerializer extends BooleanDefaultsSerializer {
        public UseExtendedAggregatePushDownSerializer() {
            super(DEFAULT_USE_EXTENDED_AGGREGATE_PUSH_DOWN);
        }
    }

}
