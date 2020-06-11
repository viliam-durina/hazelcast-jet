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

import com.hazelcast.jet.sql.impl.opt.logical.InsertLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableModify;

import static com.hazelcast.sql.impl.calcite.opt.HazelcastConventions.LOGICAL;
import static com.hazelcast.sql.impl.calcite.opt.HazelcastConventions.PHYSICAL;

public final class InsertPhysicalRule extends ConverterRule {

    public static final RelOptRule INSTANCE = new InsertPhysicalRule();

    private InsertPhysicalRule() {
        super(
                InsertLogicalRel.class, TableModify::isInsert, LOGICAL, PHYSICAL,
                InsertPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public RelNode convert(RelNode rel) {
        InsertLogicalRel tableModify = (InsertLogicalRel) rel;

        return new InsertPhysicalRel(
                tableModify.getCluster(),
                OptUtils.toPhysicalConvention(tableModify.getTraitSet()),
                tableModify.getTable(),
                tableModify.getCatalogReader(),
                OptUtils.toPhysicalInput(tableModify.getInput()),
                tableModify.getOperation(),
                tableModify.getUpdateColumnList(),
                tableModify.getSourceExpressionList(),
                tableModify.isFlattened()
        );
    }
}
