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

import com.hazelcast.jet.sql.impl.opt.AbstractFullScanRel;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.map.AbstractMapTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;

public final class JetProjectIntoScanLogicalRule extends RelOptRule {

    public static final RelOptRule INSTANCE = new JetProjectIntoScanLogicalRule();

    private JetProjectIntoScanLogicalRule() {
        super(
                operand(Project.class, operand(AbstractFullScanRel.class, any())), RelFactories.LOGICAL_BUILDER,
                JetProjectIntoScanLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Project project = call.rel(0);
        TableScan scan = call.rel(1);
        HazelcastTable table = scan.getTable().unwrap(HazelcastTable.class);
        if (table.getTarget() instanceof AbstractMapTable) {
            return;
        }

        ConnectorScanLogicalRel fullScanRel = new ConnectorScanLogicalRel(
                scan.getCluster(),
                OptUtils.toLogicalConvention(scan.getTraitSet()),
                scan.getTable(),
                project.getProjects(),
                getScanFilter(scan)
        );
        call.transformTo(fullScanRel);
    }

    /**
     * Get filter associated with the scan, if any.
     *
     * @param scan Scan.
     * @return Filter or null.
     */
    private static RexNode getScanFilter(TableScan scan) {
        return scan instanceof ConnectorScanLogicalRel ? ((ConnectorScanLogicalRel) scan).getFilter() : null;
    }
}
