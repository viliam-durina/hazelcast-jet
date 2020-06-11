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

import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.opt.AbstractFullScanRel;
import com.hazelcast.jet.sql.impl.opt.physical.visitor.CreateDagVisitor;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * A scan over a source connector. It might be a full scan, index scan or
 * other, depending on what the connector is able to do with the
 * filter/projection.
 */
public class ConnectorScanPhysicalRel extends AbstractFullScanRel implements JetPhysicalRel {

    public ConnectorScanPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            List<RexNode> projection,
            RexNode filter
    ) {
        super(cluster, traitSet, table, projection, filter);
    }

    public Expression<Boolean> filter() {
        PlanNodeSchema schema = new PlanNodeSchema(Util.toList(getTableUnwrapped().getFields(), TableField::getType));
        return filter(schema, getFilter());
    }

    public List<Expression<?>> projection() {
        PlanNodeSchema schema = new PlanNodeSchema(Util.toList(getTableUnwrapped().getFields(), TableField::getType));
        return project(schema, getProjection());
    }

    @Override
    public PlanNodeSchema schema() {
        List<QueryDataType> fieldTypes = projection().stream()
                .map(Expression::getType)
                .collect(toList());
        return new PlanNodeSchema(fieldTypes);
    }

    @Override
    public void visit0(CreateDagVisitor visitor) {
        visitor.onFullScan(this);
    }

    @Override
    public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new ConnectorScanPhysicalRel(getCluster(), traitSet, getTable(), getProjection(), getFilter());
    }
}
