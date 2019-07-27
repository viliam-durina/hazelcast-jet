/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.TerminationMode;
import com.hazelcast.jet.impl.operation.SnapshotOperation.SnapshotOperationResult;
import com.hazelcast.nio.Address;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

public class TempExecutionContext implements ExecutionContext {

    private final long jobId;
    private final long executionId;

    private final ConcurrentLinkedQueue<Tuple2<Address, byte[]>> queue =
            new ConcurrentLinkedQueue<>();
    private final AtomicInteger counter = new AtomicInteger();
    private final CountDownLatch latch = new CountDownLatch(1);

    public TempExecutionContext(long jobId, long executionId) {
        this.jobId = jobId;
        this.executionId = executionId;
    }

    void block() {
        counter.updateAndGet(i -> {
            assert i >= 0 : "i=" + i;
            return -i;
        });
    }

    @Override
    public void completeExecution(Throwable error) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public CompletableFuture<Void> terminateExecution(@Nullable TerminationMode mode) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public CompletableFuture<SnapshotOperationResult> beginSnapshot(long snapshotId, String mapName, boolean isTerminal) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void handlePacket(byte[] packet, Address sender) throws ContextSwappedException {
        int counter = this.counter.updateAndGet(i -> {
            if (i >= 0) {
                i++;
            }
            return i;
        });
        if (counter > 0) {
            queue.add(tuple2(sender, packet));
        } else {
            // we block the networking thread here...
            latch.await();
            throw new ContextSwappedException();
        }
    }

    @Override
    public long jobId() {
        return jobId;
    }

    @Override
    public long executionId() {
        return executionId;
    }

    public static class ContextSwappedException extends Exception {
    }
}
