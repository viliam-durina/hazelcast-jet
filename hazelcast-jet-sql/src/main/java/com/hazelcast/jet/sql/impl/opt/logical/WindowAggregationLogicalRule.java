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

import com.google.common.collect.ImmutableList;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlDescriptorOperator;
import org.apache.calcite.sql.SqlHopTableFunction;
import org.apache.calcite.sql.SqlTumbleTableFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableBitSet.Builder;
import org.apache.calcite.util.mapping.Mappings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

/**
 * A rule to convert:
 * - Aggregate
 * -- Project
 * --- TableFunctionScan[function=SqlTumbleTableFunction|SqlHopTableFunction]
 * ---- (any 1 input)
 *
 * into:
 * - JetWindowAggregationRel
 * -- (the input)
 */
final class WindowAggregationLogicalRule extends RelOptRule {

    static final RelOptRule INSTANCE = new WindowAggregationLogicalRule();

    private WindowAggregationLogicalRule() {
        super(
                operandJ(LogicalAggregate.class, null, aggr -> isSimpleAggregation(aggr),
                        operand(LogicalProject.class,
                                operandJ(LogicalTableFunctionScan.class, null, scan -> isTumbleOrHop(scan),
                                        operand(RelNode.class,
                                                any())))),
                RelFactories.LOGICAL_BUILDER,
                WindowAggregationLogicalRule.class.getSimpleName()
        );
    }

    private static boolean isSimpleAggregation(LogicalAggregate aggr) {
        return aggr.getGroupType() == Group.SIMPLE
                && aggr.getGroupSets().size() == 1;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalAggregate aggr = call.rel(0);
        LogicalProject prj = call.rel(1);
        LogicalTableFunctionScan windowTransform = call.rel(2);
        RelNode input = call.rel(3);

        // check that we're aggregating using win_start or win_end

        RexCall windowFuncCall = (RexCall) windowTransform.getCall();
        boolean isHop = windowFuncCall.getOperator() instanceof SqlHopTableFunction;
        int timeColumn = getDescriptorColumn(windowFuncCall.getOperands().get(0));
        RexNode windowSize = windowFuncCall.getOperands().get(1);
        RexNode windowSlide = isHop ? windowFuncCall.getOperands().get(2) : windowSize;

        // TODO check that the `timeColumn` is the watermarked one

        // last two fields of the windowTransform are window_start and window_end
        List<RelDataTypeField> windowRowType = windowTransform.getRowType().getFieldList();
        assert windowRowType.size() > 2 : windowRowType;
        // the indices of window_start and window_end fields in the output of windowTransform
        int windowStartIndex = windowRowType.size() - 2;
        int windowEndIndex = windowRowType.size() - 1;
        assert windowRowType.get(windowStartIndex).getName().equals("window_start");
        assert windowRowType.get(windowEndIndex).getName().equals("window_end");

        ImmutableBitSet newGroupSet = removeGroupingsByWindowBoundary(
                aggr.getGroupSet(), prj.getProjects(), windowStartIndex, windowEndIndex);
        if (newGroupSet == null) {
            return;
        }

        final SortedSet<Integer> interestingFields =
                (SortedSet<Integer>) RelOptUtil.getAllFields2(newGroupSet, aggr.getAggCallList());
        final Map<Integer, Integer> map = new HashMap<>();
        int i = 0;
        for (int source : interestingFields) {
            map.put(source, i++);
        }

        newGroupSet = newGroupSet.permute(map);

        final ImmutableList.Builder<AggregateCall> aggCalls = ImmutableList.builder();
        final int sourceCount = prj.getProjects().size();
        final int targetCount = interestingFields.size();
        final Mappings.TargetMapping targetMapping =
                Mappings.target(map, sourceCount, targetCount);
        for (AggregateCall aggregateCall : aggr.getAggCallList()) {
            aggCalls.add(aggregateCall.transform(targetMapping));
        }

        //        List<AggregateCall> newAggrCallList = new ArrayList<>(aggr.getAggCallList().size());
//        // remove projects that are references to window boundary and are only used for grouping
//        List<Integer> unusedProjects = new ArrayList<>();
//        for (int i = 0; i < prj.getProjects().size(); i++) {
//            RexNode expr = prj.getProjects().get(i);
//            if (expr instanceof RexInputRef) {
//                int index = ((RexInputRef) expr).getIndex();
//                int finalI = i;
//                if ((index == windowStartIndex || index == windowEndIndex)
//                        && aggr.getAggCallList().stream().flatMap(a -> a.getArgList().stream()).noneMatch(i1 -> i1 == finalI)) {
//                    unusedProjects.add(i);
//                }
//            }
//        }
//
//        // update the aggr to reflect removed projections
//        TargetMapping mapping = Mappings.target(i -> unusedProjects.indexOf(i), prj.getProjects().size(), prj.getProjects().size() - unusedProjects.size());
//        for (AggregateCall aggrCall : aggr.getAggCallList()) {
//            newAggrCallList.add(aggrCall.transform(mapping));
//        }
//        return newAggrCallList;

        List<RexNode> newProjects = Mappings.permute(prj.getProjects(), targetMapping.inverse());
        List<String> newProjectsNames = Mappings.permute(prj.getRowType().getFieldNames(), targetMapping.inverse());
        LogicalProject newProjectRel = LogicalProject.create(
                OptUtils.toLogicalInput(input),
                prj.getHints(),
                newProjects,
                newProjectsNames);

        call.transformTo(new WindowAggregationLogicalRel(
                aggr.getCluster(),
                OptUtils.toLogicalConvention(aggr.getTraitSet()),
                aggr.getHints(), // TODO collect all the hints?
                newProjectRel,
                newGroupSet,
                null,
                aggCalls.build(),
                prj.getProjects(),
                windowSize,
                windowSlide));
    }

    /**
     * @return null, if it's not a window aggregation or it's a grouping by an
     *      expression with a window boundary
     */
    @Nullable
    private ImmutableBitSet removeGroupingsByWindowBoundary(
            ImmutableBitSet groupSet,
            List<RexNode> projectExprs,
            int windowStartIndex,
            int windowEndIndex
    ) {
        // remove window_start and window_end from the grouping set. They might be there multiple times
        Builder newGroupSet = groupSet.rebuild();
        boolean windowBoundaryGroupingFound = false;
        for (Integer index : groupSet) {
            RexNode projection = projectExprs.get(index);
            if (projection instanceof RexInputRef) {
                int projectedIndex = ((RexInputRef) projection).getIndex();
                if (projectedIndex == windowStartIndex || projectedIndex == windowEndIndex) {
                    newGroupSet.clear(index);
                    windowBoundaryGroupingFound = true;
                }
            } else {
                // Check that the expression doesn't refer to the winStart or winEnd columns, we don't allow
                // grouping on expressions that refer to them except for a simple reference handled above
                boolean[] refToWindowBoundaryFound = {false};
                projection.accept(new RexVisitorImpl<Void>(true) {
                    @Override
                    public Void visitInputRef(RexInputRef inputRef) {
                        refToWindowBoundaryFound[0] = true;
                        return null;
                    }
                });
                if (refToWindowBoundaryFound[0]) {
                    return null;
                }
            }
        }

        if (!windowBoundaryGroupingFound) {
            // not grouping by either window_start or window_end, can't use window aggregation
            return null;
        }
        return newGroupSet.build();
    }

    /**
     * @return a tuple of {aggrCalls, groupSet} for the new aggregation
     */
    @Nonnull
    private Tuple2<List<AggregateCall>, ImmutableBitSet> updateAggregateCalls(
            LogicalAggregate aggr,
            ImmutableBitSet newGroupSet,
            LogicalProject prj
    ) {
        final SortedSet<Integer> interestingFields =
                (SortedSet<Integer>) RelOptUtil.getAllFields2(newGroupSet, aggr.getAggCallList());
        final Map<Integer, Integer> map = new HashMap<>();
        int i = 0;
        for (int source : interestingFields) {
            map.put(source, i++);
        }

        newGroupSet = newGroupSet.permute(map);

        final ImmutableList.Builder<AggregateCall> aggCalls = ImmutableList.builder();
        final int sourceCount = aggr.getInput().getRowType().getFieldCount();
        final int targetCount = prj.getProjects().size();
        final Mappings.TargetMapping targetMapping =
                Mappings.target(map, sourceCount, targetCount);
        for (AggregateCall aggregateCall : aggr.getAggCallList()) {
            aggCalls.add(aggregateCall.transform(targetMapping));
        }

        return Tuple2.tuple2(aggCalls.build(), newGroupSet);

//        List<AggregateCall> newAggrCallList = new ArrayList<>(aggr.getAggCallList().size());
//        // remove projects that are references to window boundary and are only used for grouping
//        List<Integer> unusedProjects = new ArrayList<>();
//        for (int i = 0; i < prj.getProjects().size(); i++) {
//            RexNode expr = prj.getProjects().get(i);
//            if (expr instanceof RexInputRef) {
//                int index = ((RexInputRef) expr).getIndex();
//                int finalI = i;
//                if ((index == windowStartIndex || index == windowEndIndex)
//                        && aggr.getAggCallList().stream().flatMap(a -> a.getArgList().stream()).noneMatch(i1 -> i1 == finalI)) {
//                    unusedProjects.add(i);
//                }
//            }
//        }
//
//        // update the aggr to reflect removed projections
//        TargetMapping mapping = Mappings.target(i -> unusedProjects.indexOf(i), prj.getProjects().size(), prj.getProjects().size() - unusedProjects.size());
//        for (AggregateCall aggrCall : aggr.getAggCallList()) {
//            newAggrCallList.add(aggrCall.transform(mapping));
//        }
//        return newAggrCallList;
    }

    private static boolean isGroupedBy(int fieldIndex, LogicalProject prj, LogicalAggregate aggr) {
        // find the fieldIndex after projection
        for (int i = 0; i < prj.getProjects().size(); i++) {
            RexNode project = prj.getProjects().get(i);
            if (project instanceof RexInputRef && ((RexInputRef) project).getIndex() == fieldIndex) {
                if (aggr.getGroupSet().get(i)) {
                    return true;
                }
            }
        }
        return false;
    }

    /*
    SELECT window_start, MAX(window_end), field1, count(*)
    FORM TUMBLE(t, DESC(time), 5 secs)
    GROUP BY window_start, field1
    -- OR
    GROUP BY field1, window_start

    SELECT window_start, window_end, field1, count(*)
    FORM TUMBLE(t, DESC(time), 5 secs)
    GROUP BY window_start, window_end, field1
     */

    private static int getDescriptorColumn(RexNode timeColumnOperand) {
        assert timeColumnOperand instanceof RexCall : timeColumnOperand;
        assert ((RexCall) timeColumnOperand).getOperator() instanceof SqlDescriptorOperator : timeColumnOperand;
        List<RexNode> operands = ((RexCall) timeColumnOperand).getOperands();
        assert operands.size() == 1 : operands.size();
        return ((RexInputRef) operands.get(0)).getIndex();
    }

    private static boolean isTumbleOrHop(LogicalTableFunctionScan scan) {
        if (scan == null || !(scan.getCall() instanceof RexCall)) {
            return false;
        }
        RexCall call = (RexCall) scan.getCall();

        return call.getOperator() instanceof SqlTumbleTableFunction
                || call.getOperator() instanceof SqlHopTableFunction;
    }
}
