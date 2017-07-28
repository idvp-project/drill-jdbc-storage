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

    private final String driver;
    private final String url;
    private final String username;
    private final String password;

    @JsonCreator
    public JdbcStorageConfig(
            @JsonProperty("driver") String driver,
            @JsonProperty("url") String url,
            @JsonProperty("username") String username,
            @JsonProperty("password") String password) {
        super();
        this.driver = driver;
        this.url = url;
        this.username = username;
        this.password = password;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JdbcStorageConfig that = (JdbcStorageConfig) o;
        return Objects.equals(driver, that.driver) &&
                Objects.equals(url, that.url) &&
                Objects.equals(username, that.username) &&
                Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(driver, url, username, password);
    }
}
