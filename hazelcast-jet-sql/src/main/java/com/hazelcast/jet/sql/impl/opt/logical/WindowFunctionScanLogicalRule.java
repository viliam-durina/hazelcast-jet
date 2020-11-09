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

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlHopTableFunction;
import org.apache.calcite.sql.SqlTumbleTableFunction;
import org.apache.calcite.sql.SqlWindowTableFunction;

import static com.hazelcast.jet.sql.impl.opt.JetConventions.LOGICAL;

final class WindowFunctionScanLogicalRule extends ConverterRule {

    static final RelOptRule INSTANCE = new WindowFunctionScanLogicalRule();

    private WindowFunctionScanLogicalRule() {
        super(
                LogicalTableFunctionScan.class, scan -> extractFunction(scan) != null,
                Convention.NONE, LOGICAL,
                WindowFunctionScanLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalTableFunctionScan scan = (LogicalTableFunctionScan) rel;

        return new WindowFunctionScanLogicalRel(
                scan.getCluster(),
                OptUtils.toLogicalConvention(scan.getTraitSet()),
                OptUtils.toLogicalInputs(scan.getInputs()),
                scan.getCall(),
                scan.getElementType(),
                scan.getRowType(),
                scan.getColumnMappings()
        );
    }

    private static SqlWindowTableFunction extractFunction(LogicalTableFunctionScan scan) {
        if (scan == null || !(scan.getCall() instanceof RexCall)) {
            return null;
        }
        RexCall call = (RexCall) scan.getCall();

        if (!(call.getOperator() instanceof SqlTumbleTableFunction || call.getOperator() instanceof SqlHopTableFunction)) {
            return null;
        }
        return (SqlWindowTableFunction) call.getOperator();
    }
}
