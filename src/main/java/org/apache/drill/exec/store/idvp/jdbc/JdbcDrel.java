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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.exec.planner.logical.DrillImplementor;
import org.apache.drill.exec.planner.logical.DrillRel;

import java.util.List;

public class JdbcDrel extends SingleRel implements DrillRel {

    JdbcDrel(RelOptCluster cluster, RelTraitSet traits, RelNode child) {
        super(cluster, traits, child);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new JdbcDrel(getCluster(), traitSet, inputs.iterator().next());
    }

    @SuppressWarnings({ "MethodDoesntCallSuperMethod", "RedundantThrows" })
    @Override
    protected Object clone() throws CloneNotSupportedException {
        return copy(getTraitSet(), getInputs());
    }

    @Override
    public LogicalOperator implement(DrillImplementor implementor) {
        throw new UnsupportedOperationException();
    }
}
