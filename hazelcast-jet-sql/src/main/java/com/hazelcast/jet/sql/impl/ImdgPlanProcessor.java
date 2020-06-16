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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.NodeServiceProvider;
import com.hazelcast.sql.impl.NodeServiceProviderImpl;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.exec.CreateExecPlanNodeVisitor;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.exec.root.RootResultConsumer;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.state.QueryStateCallback;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.util.Collections.emptyList;

public class ImdgPlanProcessor extends AbstractProcessor {
    private final PartitionIdSet partitionIdSet;
    private final PlanNode fragment;
    private Exec exec;

    private Traverser<Object[]> traverser;
    private boolean done;

    public ImdgPlanProcessor(PartitionIdSet partitionIdSet, PlanNode fragment) {
        this.partitionIdSet = partitionIdSet;
        this.fragment = fragment;
    }

    @Override
    protected void init(@Nonnull Context context) {
        HazelcastInstanceImpl hzInstance = (HazelcastInstanceImpl) context.jetInstance().getHazelcastInstance();
        NodeServiceProvider nodeServiceProvider = new NodeServiceProviderImpl(hzInstance.node.nodeEngine);
        InternalSerializationService serializationService = hzInstance.getSerializationService();
        int outboxBatchSize = 16; // TODO ??
        UUID uuid = context.jetInstance().getCluster().getLocalMember().getUuid();

        JetRootConsumer rootConsumer = new JetRootConsumer();

        CreateExecPlanNodeVisitor visitor = new CreateExecPlanNodeVisitor(
                null,
                nodeServiceProvider,
                serializationService,
                uuid,
                rootConsumer,
                16, // TODO ?
                null,
                null,
                partitionIdSet,
                outboxBatchSize
        );

        fragment.visit(visitor);
        exec = visitor.getExec();

        QueryStateCallback callback = new QueryStateCallback() {
            @Override
            public void onFragmentFinished() {
            }

            @Override
            public void cancel(Exception e) {
                throw sneakyThrow(e);
            }

            @Override
            public void checkCancelled() {
            }
        };
        exec.setup(new QueryFragmentContext(emptyList(), () -> true, callback));

        traverser = rootConsumer.traverser
                .map(this::toObjectArray);
    }

    private Object[] toObjectArray(Row row) {
        Object[] res = new Object[row.getColumnCount()];
        for (int i = 0; i < res.length; i++) {
            res[i] = row.get(i);
        }
        return res;
    }

    @Override
    public boolean complete() {
        if (!emitFromTraverser(traverser)) {
            return false;
        }

        if (done) {
            return true;
        }

        IterationResult res = exec.advance();
        if (res == IterationResult.WAIT) {
            return false;
        } else if (res == IterationResult.FETCHED_DONE) {
            done = true;
        }

        return emitFromTraverser(traverser) && done;
    }

    public static class PSupplier implements ProcessorSupplier, DataSerializable {

        private PartitionIdSet partitionIdSet;
        private PlanNode fragment;

        @SuppressWarnings("unused") // for deserialization
        public PSupplier() { }

        public PSupplier(PartitionIdSet partitionIdSet, PlanNode fragment) {
            this.partitionIdSet = partitionIdSet;
            this.fragment = fragment;
        }

        @Nonnull @Override
        public Collection<? extends Processor> get(int count) {
            if (count != 1) {
                throw new RuntimeException("expected count is 1, but is " + count);
            }

            return Collections.singleton(new ImdgPlanProcessor(partitionIdSet, fragment));
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            SerializationUtil.writeNullablePartitionIdSet(partitionIdSet, out);
            out.writeObject(fragment);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            partitionIdSet = SerializationUtil.readNullablePartitionIdSet(in);
            fragment = in.readObject();
        }
    }

    public static class MetaSupplier implements ProcessorMetaSupplier, DataSerializable {

        private Map<UUID, PartitionIdSet> partitionMap;
        private Collection<UUID> fragmentMapping;
        private PlanNode fragment;

        private transient Context context;

        @SuppressWarnings("unused") // for deserialization
        public MetaSupplier() {
        }

        public MetaSupplier(Plan plan, int fragmentIndex) {
            this.partitionMap = plan.getPartitionMap();
            this.fragmentMapping = plan.getFragmentMapping(fragmentIndex).getMemberIds();
            this.fragment = plan.getFragment(fragmentIndex);
        }

        @Override
        public void init(@Nonnull Context context) {
            this.context = context;
        }

        @Override
        public int preferredLocalParallelism() {
            return 1;
        }

        @Nonnull @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            Set<Member> members = context.jetInstance().getCluster().getMembers();

            // build a temporary mapping from uuid to address
            Map<UUID, Address> uuidToAddress = new HashMap<>();
            for (Member member : members) {
                uuidToAddress.put(member.getUuid(), member.getAddress());
            }

            // create suppliers
            Map<Address, ProcessorSupplier> suppliers = new HashMap<>();
            for (UUID memberId : fragmentMapping) {
                Address address = uuidToAddress.get(memberId);
                if (address == null) {
                    throw new RuntimeException("A member supposed to run the operation is not in the cluster");
                }
                if (!addresses.contains(address)) {
                    throw new RuntimeException("The job doesn't run on a required member");
                }
                suppliers.put(address, new PSupplier(partitionMap.get(memberId), fragment));
            }

            return address -> suppliers.getOrDefault(address, ProcessorSupplier.of(Processors.noopP()));
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            // Write partition map
            out.writeInt(partitionMap.size());

            for (Map.Entry<UUID, PartitionIdSet> entry : partitionMap.entrySet()) {
                UUIDSerializationUtil.writeUUID(out, entry.getKey());
                SerializationUtil.writeNullablePartitionIdSet(entry.getValue(), out);
            }

            out.writeObject(fragmentMapping);
            out.writeObject(fragment);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            // Read partition map
            int partitionMappingCnt = in.readInt();
            partitionMap = new HashMap<>(partitionMappingCnt);

            for (int i = 0; i < partitionMappingCnt; i++) {
                partitionMap.put(
                        UUIDSerializationUtil.readUUID(in),
                        SerializationUtil.readNullablePartitionIdSet(in));
            }

            fragmentMapping = in.readObject();
            fragment = in.readObject();
        }
    }

    private static class JetRootConsumer implements RootResultConsumer {

        private final ArrayDeque<List<Row>> queue = new ArrayDeque<>(16);
        private List<Row> currentBatch;
        private int currentBatchIndex;
        private boolean last;

        private final Traverser<Row> traverser = () -> {
            if (currentBatch == null || currentBatchIndex == currentBatch.size()) {
                currentBatch = queue.poll();
                currentBatchIndex = 0;
            }
            if (currentBatch != null) {
                if (currentBatchIndex == currentBatch.size()) {
                    return null;
                }
                return currentBatch.get(currentBatchIndex++);
            }
            return null;
        };

        @Override
        public void setup(QueryFragmentContext context) {
        }

        @Override
        public boolean consume(List<Row> batch, boolean last) {
            assert !this.last;
            if (queue.size() == 16) {
                return false;
            }
            queue.add(batch);
            this.last = last;
            return true;
        }

        @Override
        public Iterator<Row> iterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onError(QueryException error) {
            throw new UnsupportedOperationException(); // TODO do we need this?
        }
    }
}
