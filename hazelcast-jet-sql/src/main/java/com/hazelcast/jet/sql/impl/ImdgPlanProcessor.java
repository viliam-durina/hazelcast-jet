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
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.sql.impl.NodeServiceProvider;
import com.hazelcast.sql.impl.NodeServiceProviderImpl;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.exec.CreateExecPlanNodeVisitor;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.exec.root.RootResultConsumer;
import com.hazelcast.sql.impl.plan.ImdgPlan;
import com.hazelcast.sql.impl.plan.PlanFragmentMapping;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.state.QueryStateCallback;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;

import javax.annotation.Nonnull;
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
    private final ImdgPlan plan;
    private final int fragmentIndex;

    private Exec exec;

    private Traverser<Object[]> traverser;
    private boolean done;

    public ImdgPlanProcessor(ImdgPlan plan, int fragmentIndex) {
        this.plan = plan;
        this.fragmentIndex = fragmentIndex;
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
                plan.getPartitionMap().get(uuid),
                outboxBatchSize
        );

        plan.getFragment(fragmentIndex).visit(visitor);
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

    public static class PSupplier implements ProcessorSupplier {

        private final ImdgPlan plan;
        private final int fragmentIndex;

        private transient boolean runsOnThisMember;

        public PSupplier(ImdgPlan plan, int fragmentIndex) {
            this.plan = plan;
            this.fragmentIndex = fragmentIndex;
        }

        @Override
        public void init(@Nonnull Context context) {
            UUID localMemberId = context.jetInstance().getHazelcastInstance().getCluster().getLocalMember().getUuid();
            runsOnThisMember = plan.getFragmentMapping(fragmentIndex).getMemberIds().contains(localMemberId);
        }

        @Nonnull @Override
        public Collection<? extends Processor> get(int count) {
            if (count != 1) {
                throw new RuntimeException("expected count is 1, but is " + count);
            }

            if (runsOnThisMember) {
                return Collections.singleton(new ImdgPlanProcessor(plan, fragmentIndex));
            } else {
                return Collections.singleton(Processors.noopP().get());
            }
        }
    }

    public static class MetaSupplier implements ProcessorMetaSupplier {

        private final ImdgPlan plan;
        private final int fragmentIndex;

        private transient Context context;

        public MetaSupplier(ImdgPlan plan, int fragmentIndex) {
            this.plan = plan;
            this.fragmentIndex = fragmentIndex;
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
            PlanFragmentMapping fragmentMapping = plan.getFragmentMapping(fragmentIndex);

            // build a temporary mapping from uuid to address
            Map<UUID, Address> uuidToAddress = new HashMap<>();
            for (Member member : members) {
                uuidToAddress.put(member.getUuid(), member.getAddress());
            }

            // check that all members in the fragment mapping are present in the given addresses
            for (UUID memberId : fragmentMapping.getMemberIds()) {
                Address address = uuidToAddress.get(memberId);
                if (address == null) {
                    throw new RuntimeException("A member supposed to run the local map part is gone");
                }
                if (!addresses.contains(address)) {
                    throw new RuntimeException("The job doesn't run on a required member");
                }
            }

            return address -> new PSupplier(plan, 0);
        }
    }

    private static class JetRootConsumer implements RootResultConsumer {

        private final ArrayDeque<List<Row>> queue = new ArrayDeque<>(16);
        private List<Row> currentBatch;
        private int currentBatchIndex;
        private boolean last;

        private final Traverser<Row> traverser = () -> {
            if (currentBatch == null) {
                currentBatch = queue.poll();
                currentBatchIndex = 0;
            }
            if (currentBatch != null) {
                try {
                    return currentBatch.get(currentBatchIndex);
                } finally {
                    currentBatchIndex++;
                    if (currentBatchIndex == currentBatch.size())
                        currentBatch = null;
                }
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
