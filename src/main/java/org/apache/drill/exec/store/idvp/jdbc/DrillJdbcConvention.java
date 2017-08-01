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

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.linq4j.tree.ConstantUntypedNull;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.sql.SqlDialect;

import java.util.Set;

/**
 * @author Oleg Zinoviev
 * @since 01.08.2017.
 */
public class DrillJdbcConvention extends JdbcConvention {

    private static final JdbcPrule JDBC_PRULE_INSTANCE = new JdbcPrule();
    // Rules from Calcite's JdbcRules class that we want to avoid using.
    private static String[] RULES_TO_AVOID = {
            "JdbcToEnumerableConverterRule", "JdbcFilterRule", "JdbcProjectRule"
    };

    private JdbcStoragePlugin plugin;
    private final ImmutableSet<RelOptRule> rules;

    DrillJdbcConvention(JdbcStoragePlugin plugin, SqlDialect dialect, String name) {
        super(dialect, ConstantUntypedNull.INSTANCE, name);
        this.plugin = plugin;


        // build rules for this convention.
        ImmutableSet.Builder<RelOptRule> builder = ImmutableSet.builder();

        builder.add(JDBC_PRULE_INSTANCE);
        builder.add(new JdbcDrelConverterRule(this));
        builder.add(new DrillJdbcRuleBase.DrillJdbcProjectRule(this));
        builder.add(new DrillJdbcRuleBase.DrillJdbcFilterRule(this));

        outside:
        for (RelOptRule rule : JdbcRules.rules(this)) {
            final String description = rule.toString();

            // we want to black list some rules but the parent Calcite package is all or none.
            // Therefore, we remove rules with names we don't like.
            for (String black : RULES_TO_AVOID) {
                if (description.equals(black)) {
                    continue outside;
                }

            }

            builder.add(rule);
        }

        builder.add(FilterSetOpTransposeRule.INSTANCE);
        builder.add(ProjectRemoveRule.INSTANCE);

        rules = builder.build();
    }

    @Override
    public void register(RelOptPlanner planner) {
        for (RelOptRule rule : rules) {
            planner.addRule(rule);
        }
    }

    Set<RelOptRule> getRules() {
        return rules;
    }

    JdbcStoragePlugin getPlugin() {
        return plugin;
    }
}
