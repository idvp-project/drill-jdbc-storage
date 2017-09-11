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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.StoragePluginConfig;

import java.util.Objects;

@JsonTypeName(JdbcStorageConfig.NAME)
public class JdbcStorageConfig extends StoragePluginConfig {

    static final String NAME = "jdbc-idvp";
    private static final int DEFAULT_POOL_SIZE = 64;
    private static final int DEFAULT_VALIDATION_TIMEOUT = 500;
    private static final boolean DEFAULT_USE_STANDARD_DIALECT = false;

    private final String driver;
    private final String url;
    private final String username;
    private final String password;

    private final int connectionPoolSize;
    private final int connectionValidationTimeout;

    private final boolean useStandardDialect;

    @JsonCreator
    public JdbcStorageConfig(
            @JsonProperty("driver") String driver,
            @JsonProperty("url") String url,
            @JsonProperty("username") String username,
            @JsonProperty("password") String password,
            @JsonProperty("connectionPoolSize") Integer connectionPoolSize,
            @JsonProperty("connectionValidationTimeout") Integer connectionValidationTimeout,
            @JsonProperty("useStandardDialect") Boolean useStandardDialect) {
        super();
        this.driver = driver;
        this.url = url;
        this.username = username;
        this.password = password;
        this.connectionPoolSize = connectionPoolSize == null ? DEFAULT_POOL_SIZE : connectionPoolSize;
        this.connectionValidationTimeout = connectionValidationTimeout == null ? DEFAULT_VALIDATION_TIMEOUT : connectionValidationTimeout;
        this.useStandardDialect = useStandardDialect == null ? DEFAULT_USE_STANDARD_DIALECT : useStandardDialect;
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
    public int getConnectionPoolSize() {
        return connectionPoolSize;
    }

    @JsonProperty
    public int getConnectionValidationTimeout() {
        return connectionValidationTimeout;
    }

    @JsonProperty
    public boolean isUseStandardDialect() {
        return useStandardDialect;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JdbcStorageConfig that = (JdbcStorageConfig) o;
        return connectionPoolSize == that.connectionPoolSize &&
                Objects.equals(driver, that.driver) &&
                Objects.equals(url, that.url) &&
                Objects.equals(username, that.username) &&
                Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(driver, url, username, password, connectionPoolSize);
    }
}
