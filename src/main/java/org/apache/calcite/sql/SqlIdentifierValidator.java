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

import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Oleg Zinoviev
 * @since 01.06.18
 */
class SqlIdentifierValidator {
    private final Pattern identifierPattern;
    private final Set<String> reserved;

    SqlIdentifierValidator(DataSource dataSource) throws SQLException, IOException {
        DatabaseMetaData metaData = dataSource.getConnection().getMetaData();
        this.identifierPattern = createIdentifierPattern(metaData);
        this.reserved = createReserved(metaData);
    }

    boolean identifierNeedsToBeQuoted(String val) {
        return !identifierPattern.matcher(val).matches() || reserved.contains(val);
    }

    private Pattern createIdentifierPattern(DatabaseMetaData metaData) throws SQLException {
        final StringBuilder identifierPatternBuilder = new StringBuilder("^[A-Za-z_0-9");
        String extraNameCharacters = metaData.getExtraNameCharacters();
        for (char extraCharacter : extraNameCharacters.toCharArray()) {
            identifierPatternBuilder.append(Pattern.quote(String.valueOf(extraCharacter)));
        }

        identifierPatternBuilder.append("]+$");

        return Pattern.compile(identifierPatternBuilder.toString());
    }

    private Set<String> createReserved(DatabaseMetaData metaData) throws SQLException, IOException {

        try (InputStream stream = SqlIdentifierValidator.class.getResourceAsStream("/sql2003-reserved.txt")) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String sqlKeywords = metaData.getSQLKeywords();

                return Stream.concat(
                        Pattern.compile(",").splitAsStream(sqlKeywords),
                        reader.lines()
                )
                        .filter(StringUtils::isNotBlank)
                        .collect(Collectors.toCollection(() -> new TreeSet<>(String::compareToIgnoreCase)));
            }
        }

    }
}
