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

package com.hazelcast.jet.sql.impl.opt;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import static com.hazelcast.sql.impl.calcite.opt.HazelcastConventions.LOGICAL;
import static com.hazelcast.sql.impl.calcite.opt.HazelcastConventions.PHYSICAL;
import static org.apache.calcite.plan.RelOptRule.convert;

/**
 * Static utility classes for rules.
 */
public final class OptUtils {

    private OptUtils() {
    }

    /**
     * Get operand matching a single node.
     *
     * @param cls        Node class.
     * @param convention Convention.
     * @return Operand.
     */
    public static <R extends RelNode> RelOptRuleOperand single(Class<R> cls, Convention convention) {
        return RelOptRule.operand(cls, convention, RelOptRule.any());
    }

    /**
     * Get operand matching a node with specific child node.
     *
     * @param cls        Node class.
     * @param childCls   Child node class.
     * @param convention Convention.
     * @return Operand.
     */
    public static <R1 extends RelNode, R2 extends RelNode> RelOptRuleOperand parentChild(
            Class<R1> cls,
            Class<R2> childCls, Convention convention
    ) {
        RelOptRuleOperand childOperand = RelOptRule.operand(childCls, RelOptRule.any());

        return RelOptRule.operand(cls, convention, RelOptRule.some(childOperand));
    }

    /**
     * Get operand matching a node with specific pair of child nodes.
     *
     * @param cls        Node class.
     * @param childCls1  Child node class 1.
     * @param childCls2  Child node class 2.
     * @param convention Convention.
     * @return Operand.
     */
    public static <R1 extends RelNode, R2 extends RelNode, R3 extends RelNode> RelOptRuleOperand parentChildChild(
            Class<R1> cls,
            Class<R2> childCls1,
            Class<R3> childCls2,
            Convention convention
    ) {
        RelOptRuleOperand childOperand1 = RelOptRule.operand(childCls1, RelOptRule.any());
        RelOptRuleOperand childOperand2 = RelOptRule.operand(childCls2, RelOptRule.any());

        return RelOptRule.operand(cls, convention, RelOptRule.some(childOperand1, childOperand2));
    }

    /**
     * Add a single trait to the trait set.
     *
     * @param traitSet Original trait set.
     * @param trait    Trait to add.
     * @return Resulting trait set.
     */
    public static RelTraitSet traitPlus(RelTraitSet traitSet, RelTrait trait) {
        return traitSet.plus(trait).simplify();
    }

    /**
     * Convert the given trait set to logical convention.
     *
     * @param traitSet Original trait set.
     * @return New trait set with logical convention.
     */
    public static RelTraitSet toLogicalConvention(RelTraitSet traitSet) {
        return traitPlus(traitSet, LOGICAL);
    }

    /**
     * Convert the given input into logical input.
     *
     * @param input Original input.
     * @return Logical input.
     */
    public static RelNode toLogicalInput(RelNode input) {
        return convert(input, toLogicalConvention(input.getTraitSet()));
    }

    /**
     * Convert the given trait set to physical convention.
     *
     * @param traitSet Original trait set.
     * @return New trait set with physical convention and provided distribution.
     */
    public static RelTraitSet toPhysicalConvention(RelTraitSet traitSet) {
        return traitPlus(traitSet, PHYSICAL);
    }

    /**
     * Convert the given input into physical input.
     *
     * @param input Original input.
     * @return Logical input.
     */
    public static RelNode toPhysicalInput(RelNode input) {
        return convert(input, toPhysicalConvention(input.getTraitSet()));
    }
}
