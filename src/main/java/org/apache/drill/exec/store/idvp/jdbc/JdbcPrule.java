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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.physical.Prel;

/**
 * @author Oleg Zinoviev
 * @since 01.08.2017.
 */
public class JdbcPrule extends ConverterRule {

    JdbcPrule() {
        super(JdbcDrel.class, DrillRel.DRILL_LOGICAL, Prel.DRILL_PHYSICAL, "JDBC_PREL_Converter");
    }

    @Override
    public RelNode convert(RelNode in) {

        return new JdbcIntermediatePrel(
                in.getCluster(),
                in.getTraitSet().replace(getOutTrait()),
                in.getInput(0));
    }

}
