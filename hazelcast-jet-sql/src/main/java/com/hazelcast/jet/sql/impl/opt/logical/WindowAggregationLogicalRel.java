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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

public class WindowAggregationLogicalRel extends Aggregate implements LogicalRel {

    private final int timeColumn;
    private final List<RexNode> projects;
    private final RexNode windowSize;
    private final RexNode windowSlide;

    protected WindowAggregationLogicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            int timeColumn,
            List<RexNode> projects,
            RexNode windowSize,
            RexNode windowSlide
    ) {
        super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
        this.timeColumn = timeColumn;

        this.projects = projects;
        this.windowSize = windowSize;
        this.windowSlide = windowSlide;
    }

    @Override
    public Aggregate copy(
            RelTraitSet traitSet,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls
    ) {
        throw new UnsupportedOperationException();
        //return new WindowAggregationLogicalRel(getCluster(), traitSet, getHints(), input, groupSet, groupSets, aggCalls, prj.getProjects(), windowSize, windowSlide);
    }

    public int getTimeColumn() {
        return timeColumn;
    }

    public List<RexNode> getProjects() {
        return projects;
    }

    public RexNode getWindowSize() {
        return windowSize;
    }

    public RexNode getWindowSlide() {
        return windowSlide;
    }
}
