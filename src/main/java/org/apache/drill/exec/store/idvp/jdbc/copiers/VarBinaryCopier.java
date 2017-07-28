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

import org.apache.drill.exec.vector.NullableVarBinaryVector;

import java.sql.ResultSet;
import java.sql.SQLException;


public class VarBinaryCopier extends Copier<NullableVarBinaryVector.Mutator> {

    public VarBinaryCopier(int columnIndex, ResultSet result, NullableVarBinaryVector.Mutator mutator) {
        super(columnIndex, result, mutator);
    }

    @Override
    public void copy(int index) throws SQLException {
        byte[] record = result.getBytes(columnIndex);
        if (record != null) {
            mutator.setSafe(index, record, 0, record.length);
        }
    }

}
