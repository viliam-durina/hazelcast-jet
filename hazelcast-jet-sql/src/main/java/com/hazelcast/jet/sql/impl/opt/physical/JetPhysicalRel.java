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
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.PhysicalRelVisitor;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.RexToExpressionVisitor;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.rex.RexNode;

import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * Marker interface for logical relations.
 */
public interface JetPhysicalRel extends PhysicalRel {

    // TODO: moved to PlanNode as part of CreateDagVisitor ???
    PlanNodeSchema schema();

    // TODO: moved to PlanNode as part of CreateDagVisitor ???
    @SuppressWarnings("unchecked")
    default Expression<Boolean> filter(PlanNodeSchema schema, RexNode node) {
        if (node == null) {
            return null;
        }
        // TODO: pass actual parameter metadata
        RexToExpressionVisitor converter = new RexToExpressionVisitor(schema, new QueryParameterMetadata());
        return (Expression<Boolean>) node.accept(converter);
    }

    // TODO: moved to PlanNode as part of CreateDagVisitor ???
    default List<Expression<?>> project(PlanNodeSchema schema, List<RexNode> nodes) {
        // TODO: pass actual parameter metadata
        RexToExpressionVisitor converter = new RexToExpressionVisitor(schema, new QueryParameterMetadata());
        return nodes.stream()
                    .map(node -> (Expression<?>) node.accept(converter))
                    .collect(toList());
    }

    /**
     * Visit physical rel. A node with children should delegate to parent nodes first.
     *
     * @param visitor Visitor.
     */
    void visit0(CreateDagVisitor visitor);

    default void visit(PhysicalRelVisitor visitor) {
        visit0((CreateDagVisitor) visitor);
    }
}
