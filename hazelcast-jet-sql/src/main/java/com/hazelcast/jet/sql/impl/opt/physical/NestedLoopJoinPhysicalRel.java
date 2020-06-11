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

import com.hazelcast.jet.sql.impl.opt.physical.visitor.CreateDagVisitor;
import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import java.util.Collections;

public class NestedLoopJoinPhysicalRel extends Join implements JetPhysicalRel {

    public NestedLoopJoinPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            JoinRelType joinType
    ) {
        super(cluster, traitSet, left, right, condition, Collections.emptySet(), joinType);

        assert joinType == JoinRelType.INNER : "Only INNER join is supported"; // TODO: what about other join types?
    }

    public Expression<Boolean> condition() {
        return filter(schema(), getCondition());
    }

    @Override
    public PlanNodeSchema schema() {
        PlanNodeSchema leftSchema = ((JetPhysicalRel) getLeft()).schema();
        PlanNodeSchema rightSchema = ((JetPhysicalRel) getRight()).schema();
        return PlanNodeSchema.combine(leftSchema, rightSchema);
    }

    @Override
    public void visit0(CreateDagVisitor visitor) {
        visitor.onNestedLoopRead(this);

        ((PhysicalRel) getLeft()).visit(visitor);
    }

    @Override
    public Join copy(
            RelTraitSet traitSet,
            RexNode conditionExpr,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone
    ) {
        return new NestedLoopJoinPhysicalRel(
                getCluster(),
                traitSet,
                left,
                right,
                condition,
                joinType
        );
    }
}
