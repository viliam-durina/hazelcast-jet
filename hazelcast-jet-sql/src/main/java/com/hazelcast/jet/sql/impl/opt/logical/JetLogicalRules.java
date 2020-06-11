/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.sql.impl.calcite.opt.logical.FilterIntoScanLogicalRule;
import com.hazelcast.sql.impl.calcite.opt.logical.MapScanLogicalRule;
import com.hazelcast.sql.impl.calcite.opt.logical.ProjectIntoScanLogicalRule;
import com.hazelcast.sql.impl.calcite.opt.logical.ProjectLogicalRule;
import org.apache.calcite.rel.rules.FilterJoinRule.FilterIntoJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ValuesReduceRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public final class JetLogicalRules {

    private JetLogicalRules() {
    }

    public static RuleSet getRuleSet() {
        // TODO: Use HEP instead?
        return RuleSets.ofList(
                // Join optimization rules.
                // new FilterJoinRule.FilterIntoJoinRule(true, RelFactories.LOGICAL_BUILDER, FilterPredicate.INSTANCE),
                // new FilterJoinRule.JoinConditionPushRule(RelFactories.LOGICAL_BUILDER, FilterPredicate.INSTANCE),
                // JoinPushExpressionsRule.INSTANCE,

                // Filter and project rules.
                FilterMergeRule.INSTANCE,
                ProjectMergeRule.INSTANCE,
                FilterProjectTransposeRule.INSTANCE,
                // TODO: ProjectMergeRule: https://jira.apache.org/jira/browse/CALCITE-2223
                ProjectFilterTransposeRule.INSTANCE,
                // ProjectJoinTransposeRule.INSTANCE,
                ProjectRemoveRule.INSTANCE,
                JetProjectIntoScanLogicalRule.INSTANCE,
                JetFilterIntoScanLogicalRule.INSTANCE,
                FilterIntoJoinRule.FILTER_ON_JOIN,
                ValuesReduceRule.FILTER_INSTANCE,
                ValuesReduceRule.PROJECT_FILTER_INSTANCE,
                ValuesReduceRule.PROJECT_INSTANCE,

                // TODO: Aggregate rules

                // SemiJoinRule.PROJECT,
                // SemiJoinRule.JOIN,

                // Convert Calcite node into Hazelcast nodes.
                // TODO: Should we extend converter here instead (see Flink)?
                ValuesLogicalRule.INSTANCE,
                InsertLogicalRule.INSTANCE,
                FullScanLogicalRule.INSTANCE,
                ProjectLogicalRule.INSTANCE,

                // AggregateLogicalRule.INSTANCE,
                // SortLogicalRule.INSTANCE,
                JoinLogicalRule.INSTANCE,

                // TODO: Transitive closures:
                //  (a.a=b.b) AND (a=1) -> (a.a=b.b) AND (a=1) AND (b=1) -> pushdown to two tables, not one

                // IMDG rules: local imap/replicatedMap scanning
                MapScanLogicalRule.INSTANCE,
                ProjectIntoScanLogicalRule.INSTANCE,
                FilterIntoScanLogicalRule.INSTANCE
        );
    }
}
