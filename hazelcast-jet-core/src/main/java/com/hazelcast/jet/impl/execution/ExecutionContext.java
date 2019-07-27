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

import com.hazelcast.jet.impl.TerminationMode;
import com.hazelcast.jet.impl.execution.TempExecutionContext.ContextSwappedException;
import com.hazelcast.jet.impl.operation.SnapshotOperation.SnapshotOperationResult;
import com.hazelcast.nio.Address;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public interface ExecutionContext {
    void completeExecution(Throwable error);

    CompletableFuture<Void> terminateExecution(@Nullable TerminationMode mode);

    CompletableFuture<SnapshotOperationResult> beginSnapshot(long snapshotId, String mapName,
                                                             boolean isTerminal);

    void handlePacket(byte[] packet, Address sender) throws IOException, ContextSwappedException;

    long jobId();

    long executionId();
}
