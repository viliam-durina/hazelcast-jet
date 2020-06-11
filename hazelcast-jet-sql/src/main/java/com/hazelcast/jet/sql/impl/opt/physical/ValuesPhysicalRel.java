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

import com.google.common.collect.ImmutableList;
import com.hazelcast.jet.sql.impl.opt.physical.visitor.CreateDagVisitor;
import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

import java.util.List;

import static java.util.stream.Collectors.toList;

public class ValuesPhysicalRel extends Values implements JetPhysicalRel {

    ValuesPhysicalRel(
            RelOptCluster cluster,
            RelDataType rowType,
            ImmutableList<ImmutableList<RexLiteral>> tuples,
            RelTraitSet traitSet
    ) {
        super(cluster, rowType, tuples, traitSet);
    }

    @Override
    public PlanNodeSchema schema() {
        List<QueryDataType> fieldTypes = getRowType().getFieldList().stream()
                                                     .map(field -> SqlToQueryType.map(field.getType().getSqlTypeName()))
                                                     .collect(toList());
        return new PlanNodeSchema(fieldTypes);
    }

    @Override
    public void visit0(CreateDagVisitor visitor) {
        visitor.onValues(this);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return new ValuesPhysicalRel(getCluster(), rowType, tuples, traitSet);
    }
}
