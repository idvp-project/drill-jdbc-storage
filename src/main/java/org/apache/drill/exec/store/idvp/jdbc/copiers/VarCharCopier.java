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
package org.apache.drill.exec.store.idvp.jdbc.copiers;

import com.google.common.base.Charsets;
import org.apache.drill.exec.vector.NullableVarCharVector;

import java.sql.ResultSet;
import java.sql.SQLException;


public class VarCharCopier extends Copier<NullableVarCharVector.Mutator> {

    private final ResultSet resultSet;

    public VarCharCopier(ResultSet resultSet, int columnIndex, ResultSet result, NullableVarCharVector.Mutator mutator) {
        super(columnIndex, result, mutator);
        this.resultSet = resultSet;
    }

    @Override
    public void copy(int index) throws SQLException {
        String val = resultSet.getString(columnIndex);
        if (val != null) {
            byte[] record = val.getBytes(Charsets.UTF_8);
            mutator.setSafe(index, record, 0, record.length);
        }
    }

}
