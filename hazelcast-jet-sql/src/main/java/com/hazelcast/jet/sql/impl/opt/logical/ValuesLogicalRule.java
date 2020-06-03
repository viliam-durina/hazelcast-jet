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

import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalValues;

import static com.hazelcast.sql.impl.calcite.opt.HazelcastConventions.LOGICAL;

public final class ValuesLogicalRule extends ConverterRule {

    public static final RelOptRule INSTANCE = new ValuesLogicalRule();

    private ValuesLogicalRule() {
        super(
                LogicalValues.class, Convention.NONE, LOGICAL,
                ValuesLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public RelNode convert(RelNode rel) {
        Values values = (Values) rel;

        return new ValuesLogicalRel(
                values.getCluster(),
                values.getRowType(),
                values.getTuples(),
                OptUtils.toLogicalConvention(values.getTraitSet())
        );
    }
}
