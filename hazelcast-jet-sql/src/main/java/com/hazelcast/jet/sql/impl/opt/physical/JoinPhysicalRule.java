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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.sql.impl.opt.logical.JoinLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

import static com.hazelcast.sql.impl.calcite.opt.HazelcastConventions.LOGICAL;

public final class JoinPhysicalRule extends RelOptRule {

    public static final RelOptRule INSTANCE = new JoinPhysicalRule();

    private JoinPhysicalRule() {
        super(
                OptUtils.parentChildChild(JoinLogicalRel.class, RelNode.class, RelNode.class, LOGICAL),
                JoinPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        JoinLogicalRel logicalJoin = call.rel(0);

        // TODO: isn't it too simple?
        RelNode transform = new NestedLoopJoinPhysicalRel(
                logicalJoin.getCluster(),
                OptUtils.toPhysicalConvention(logicalJoin.getTraitSet()),
                OptUtils.toPhysicalInput(logicalJoin.getLeft()),
                OptUtils.toPhysicalInput(logicalJoin.getRight()),
                logicalJoin.getCondition(),
                logicalJoin.getJoinType()
        );

        call.transformTo(transform);
    }
}
