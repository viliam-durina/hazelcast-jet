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

package com.hazelcast.jet.sql.impl.opt.physical.visitor;

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.ImdgPlanProcessor;
import com.hazelcast.jet.sql.impl.expression.ExpressionUtil;
import com.hazelcast.jet.sql.impl.opt.physical.ConnectorScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.InsertPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.NestedLoopJoinPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.ProjectPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.ValuesPhysicalRel;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.calcite.opt.AbstractScanRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MapIndexScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ReplicatedMapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ReplicatedToDistributedPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.RootPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.NodeIdVisitor;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.PlanCreateVisitor;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.schema.Table;
import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.NlsString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.convenientSourceP;
import static com.hazelcast.jet.impl.util.Util.toList;
import static com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil.getJetSqlConnector;

public class CreateDagVisitor extends ThrowingPhysicalRelVisitor {

    private final NodeEngine nodeEngine;
    private final DAG dag;
    private final Deque<VertexAndOrdinal> vertexStack;

    public CreateDagVisitor(@Nonnull NodeEngine nodeEngine, @Nonnull DAG dag, @Nullable Vertex sink) {
        this.nodeEngine = nodeEngine;
        this.dag = dag;
        this.vertexStack = new ArrayDeque<>();

        if (sink != null) {
            vertexStack.push(new VertexAndOrdinal(sink));
        }
    }

    public void onValues(ValuesPhysicalRel rel) {
        List<Object[]> items = toList(rel.getTuples(), tuple -> tuple.stream().map(rexLiteral -> {
            Comparable<?> value = rexLiteral.getValue();
            if (value instanceof NlsString) {
                NlsString nlsString = (NlsString) value;
                assert nlsString.getCharset().name().equals(ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
                return nlsString.getValue();
            }
            return value;
        }).toArray());

        Vertex vertex = dag.newVertex("values-src", convenientSourceP(
                pCtx -> null,
                (ignored, buf) -> {
                    items.forEach(buf::add);
                    buf.close();
                },
                ctx -> null,
                (ctx, states) -> {
                },
                ConsumerEx.noop(),
                1,
                true)
        );

        push(vertex);
    }

    public void onInsert(InsertPhysicalRel rel) {
        Table table = rel.getTable().unwrap(HazelcastTable.class).getTarget();

        Vertex vertex = getJetSqlConnector(table).sink(dag, table);
        vertexStack.push(new VertexAndOrdinal(vertex));
    }

    public void onFullScan(ConnectorScanPhysicalRel rel) {
        Table table = rel.getTableUnwrapped();

        Vertex vertex = getJetSqlConnector(table)
                .fullScanReader(dag, table, null, rel.filter(), rel.projection());
        push(vertex);
    }

    public void onNestedLoopRead(NestedLoopJoinPhysicalRel rel) {
        if (rel.getRight() instanceof  ConnectorScanPhysicalRel) {
            ConnectorScanPhysicalRel rightRel = (ConnectorScanPhysicalRel) rel.getRight();

        Table table = rightRel.getTableUnwrapped();

        Vertex vertex = getJetSqlConnector(table)
                .nestedLoopReader(dag, table, rightRel.filter(), rightRel.projection(), rel.condition());
        push(vertex);
        } else if (rel.getRight() instanceof AbstractScanRel) {
            RootPhysicalRel root = new RootPhysicalRel(rel.getRight().getCluster(), rel.getRight().getTraitSet(), rel.getRight()); // TODO ?
            NodeIdVisitor idVisitor = new NodeIdVisitor();
            root.visit(idVisitor);
            Map<PhysicalRel, List<Integer>> relIdMap = idVisitor.getIdMap();

            QueryParameterMetadata parameterMetadata = QueryParameterMetadata.EMPTY;

            PlanCreateVisitor visitor = new PlanCreateVisitor(
                    nodeEngine,
                    relIdMap,
                    "", // we don't have the part of the SQL for this subtree
                    parameterMetadata
            );

            root.visit(visitor);
            Plan plan = visitor.getPlan();

            Vertex vertex = dag.newVertex("name-TODO",
                    new ImdgPlanProcessor.MetaSupplier(plan, 0)); // TODO multiple fragments
            push(vertex);
        } else {
            throw new RuntimeException("Unexpected right input: " + rel.getRight());
        }
    }

    public void onProject(ProjectPhysicalRel rel) {
        FunctionEx<Object[], Object[]> projection = ExpressionUtil.projectionFn(rel.projection());

        Vertex vertex = dag.newVertex("project", mapP(projection::apply));
        push(vertex);
    }

    @Override
    public void onMapScan(MapScanPhysicalRel rel) {
        push(imdgVertex(rel));
    }

    @Override
    public void onMapIndexScan(MapIndexScanPhysicalRel rel) {
        push(imdgVertex(rel));
    }

    @Override
    public void onReplicatedMapScan(ReplicatedMapScanPhysicalRel rel) {
        push(imdgVertex(rel));
    }

    @Override
    public void onReplicatedToDistributed(ReplicatedToDistributedPhysicalRel rel) {
        super.onReplicatedToDistributed(rel);
    }

    private Vertex imdgVertex(PhysicalRel rel) {
        RootPhysicalRel root = new RootPhysicalRel(rel.getCluster(), rel.getTraitSet(), rel); // TODO ?
        NodeIdVisitor idVisitor = new NodeIdVisitor();
        root.visit(idVisitor);
        Map<PhysicalRel, List<Integer>> relIdMap = idVisitor.getIdMap();

        QueryParameterMetadata parameterMetadata = QueryParameterMetadata.EMPTY;

        PlanCreateVisitor visitor = new PlanCreateVisitor(
                nodeEngine,
                relIdMap,
                "", // we don't have the part of the SQL for this subtree
                parameterMetadata
        );

        root.visit(visitor);
        Plan plan = visitor.getPlan();

        if (plan.getFragmentCount() != 1) {
            throw new UnsupportedOperationException("Fragment count other than 1 not yet supported (TODO)"); // TODO multiple fragments
        }

        Vertex vertex = dag.newVertex("name-TODO",
                new ImdgPlanProcessor.MetaSupplier(plan, 0));

        return vertex;
    }

    private void push(Vertex vertex) {
        assert vertex != null : "null subDag"; // we check for this earlier TODO check for it earlier :)

        VertexAndOrdinal targetVertex = vertexStack.peek();
        assert targetVertex != null : "targetVertex=null";
        dag.edge(between(vertex, targetVertex.vertex));
        targetVertex.ordinal++;
        vertexStack.push(new VertexAndOrdinal(vertex));
    }

    private static final class VertexAndOrdinal {
        final Vertex vertex;
        int ordinal;

        private VertexAndOrdinal(Vertex vertex) {
            this.vertex = vertex;
        }

        @Override
        public String toString() {
            return "{vertex=" + vertex.getName() + ", ordinal=" + ordinal + '}';
        }
    }
}
