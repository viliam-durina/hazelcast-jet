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

package com.hazelcast.jet.impl;

import reactor.blockhound.BlockHound.Builder;
import reactor.blockhound.integration.BlockHoundIntegration;

public class JetBlockHoundIntegration implements BlockHoundIntegration {
    @Override
    public void applyTo(Builder builder) {
        builder
                .nonBlockingThreadPredicate(current ->
                        current.or(t -> t.getName() != null &&
                                (t.getName().contains("jet.cooperative.thread-") || t.getName().contains(".partition-operation.thread-")))
                )
                // rules for jet cooperative threads:
                .allowBlockingCallsInside(
                        "com.hazelcast.jet.impl.execution.TaskletExecutionService$CooperativeWorker", "doIdle")
                .allowBlockingCallsInside("java.lang.ThreadGroup", "uncaughtException")
                .allowBlockingCallsInside("com.hazelcast.logging.Log4j2Factory$Log4j2Logger", "log")
                .allowBlockingCallsInside("com.hazelcast.jet.impl.execution.ProcessorTasklet", "submitToExecutor")
                .allowBlockingCallsInside("java.io.ObjectStreamClass", "lookup")

                // rules for partition threads:
                .allowBlockingCallsInside("com.hazelcast.spi.impl.operationexecutor.impl.OperationQueueImpl", "take")
                .allowBlockingCallsInside("com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl", "firstArrangement")
        ;
    }
}
