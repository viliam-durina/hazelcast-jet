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

import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.logical.ProjectLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

import static com.hazelcast.sql.impl.calcite.opt.HazelcastConventions.LOGICAL;

public final class ProjectPhysicalRule extends RelOptRule {

    public static final RelOptRule INSTANCE = new ProjectPhysicalRule();

    private ProjectPhysicalRule() {
        super(
                OptUtils.parentChild(ProjectLogicalRel.class, RelNode.class, LOGICAL),
                ProjectPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        ProjectLogicalRel logicalProject = call.rel(0);
        RelNode input = logicalProject.getInput();

        // TODO: isn't it too simple?
        ProjectPhysicalRel newProject = new ProjectPhysicalRel(
                logicalProject.getCluster(),
                OptUtils.toPhysicalConvention(input.getTraitSet()),
                OptUtils.toPhysicalInput(input),
                logicalProject.getProjects(),
                logicalProject.getRowType()
        );

        call.transformTo(newProject);
    }
}
