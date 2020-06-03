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

import com.hazelcast.sql.impl.calcite.opt.physical.FetchPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.FilterPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MapIndexScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MaterializedInputPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ProjectPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ReplicatedMapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ReplicatedToDistributedPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.RootPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.SortPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.agg.AggregatePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.BroadcastExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.RootExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.SortMergeExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.UnicastExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.join.HashJoinPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.join.NestedLoopJoinPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.PhysicalRelVisitor;

public class ThrowingPhysicalRelVisitor implements PhysicalRelVisitor {
    @Override
    public void onRoot(RootPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onMapScan(MapScanPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onMapIndexScan(MapIndexScanPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onReplicatedMapScan(ReplicatedMapScanPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onRootExchange(RootExchangePhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onUnicastExchange(UnicastExchangePhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onBroadcastExchange(BroadcastExchangePhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onSortMergeExchange(SortMergeExchangePhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onReplicatedToDistributed(ReplicatedToDistributedPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onSort(SortPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onProject(ProjectPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onFilter(FilterPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onAggregate(AggregatePhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onNestedLoopJoin(NestedLoopJoinPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onHashJoin(HashJoinPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onMaterializedInput(MaterializedInputPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onFetch(FetchPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }
}
