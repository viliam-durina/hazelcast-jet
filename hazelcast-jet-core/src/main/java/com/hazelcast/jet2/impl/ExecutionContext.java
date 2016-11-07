/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet2.impl;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.internal.util.concurrent.QueuedPipe;
import com.hazelcast.jet2.Processor;
import com.hazelcast.jet2.ProcessorSupplier;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.SimpleExecutionCallback;
import com.hazelcast.spi.partition.IPartitionService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static com.hazelcast.internal.util.concurrent.ConcurrentConveyor.concurrentConveyor;
import static com.hazelcast.jet2.impl.DoneItem.DONE_ITEM;
import static com.hazelcast.jet2.impl.OutboundCollector.compositeCollector;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

class ExecutionContext {

    private static final int QUEUE_SIZE = 1024;

    private final List<Tasklet> tasklets = new ArrayList<>();
    private final NodeEngine nodeEngine;
    private final ExecutionPlan plan;

    // vertex id --> ordinal --> receiver
    private ConcurrentMap<Integer, Map<Integer, ReceiverTasklet>> receiverMap = new ConcurrentHashMap<>();

    private final EngineContext context;

    ExecutionContext(EngineContext context, ExecutionPlan plan) {
        this.context = context;
        this.nodeEngine = context.getNodeEngine();
        this.plan = plan;
        initialize(plan);
    }

    ICompletableFuture<Void> execute() {
        Future<Void> future = context.getExecutionService().execute(tasklets);
        ICompletableFuture<Void> completable = nodeEngine
                .getExecutionService().asCompletableFuture(future);
        completable.andThen(new SimpleExecutionCallback<Void>() {
            @Override
            public void notify(Object response) {
                tasklets.clear();
            }
        });
        return completable;
    }

    void handlePacket(int vertexId, int ordinal, int partitionId,
                             byte[] buffer, int offset) {
        Map<Integer, ReceiverTasklet> ordinalMap = receiverMap.get(vertexId);
        ReceiverTasklet tasklet = ordinalMap.get(ordinal);
        byte[] data = new byte[buffer.length - offset];
        //TODO: modify HeapData to work with a byte[] and offset
        System.arraycopy(buffer, offset, data, 0, data.length);
        tasklet.offer(new HeapData(data), partitionId);
    }

    private void initialize(ExecutionPlan plan) {
        final Map<String, ConcurrentConveyor<Object>[]> conveyorMap = new HashMap<>();
        final Map<Integer, VertexDef> vMap = plan.getVertices().stream().collect(toMap(VertexDef::getId, v -> v));
        for (VertexDef vertex : plan.getVertices()) {
            final List<EdgeDef> inputs = vertex.getInputs();
            final List<EdgeDef> outputs = vertex.getOutputs();
            final int parallelism = vertex.getParallelism();
            initializePartitioner(vertex);
            List<Processor> processors = getProcessors(vertex);
            for (int taskletIndex = 0; taskletIndex < processors.size(); taskletIndex++) {
                Processor p = processors.get(taskletIndex);
                final List<InboundEdgeStream> inboundStreams = new ArrayList<>();
                final List<OutboundEdgeStream> outboundStreams = new ArrayList<>();
                for (EdgeDef edge : outputs) {
                    final int destinationId = edge.getOtherEndId();
                    final String id = vertex.getId() + ":" + edge.getOtherEndId();
                    VertexDef destination = vMap.get(destinationId);
                    final int localConsumerCount = destination.getParallelism();
                    final int receiverCount = edge.isDistributed() ? 1 : 0;

                    // each edge has an array of conveyors
                    // one conveyor per consumer - each conveyor has one queue per producer
                    // giving a total of number of producers * number of consumers queues
                    final ConcurrentConveyor<Object>[] conveyorArray = conveyorMap.computeIfAbsent(id,
                            e -> createConveyorArray(localConsumerCount, parallelism + receiverCount, QUEUE_SIZE));
                    outboundStreams.add(getOutboundEdgeStream(vertex, destination, edge, taskletIndex, conveyorArray));
                }
                for (EdgeDef input : inputs) {
                    // each tasklet will have one input conveyor per edge
                    // and one InboundEmitter per queue on the conveyor
                    final String id = input.getOtherEndId() + ":" + vertex.getId();
                    final ConcurrentConveyor<Object> conveyor = conveyorMap.get(id)[taskletIndex];
                    inboundStreams.add(getInboundEdgeStream(input, conveyor));
                }
                tasklets.add(new ProcessorTasklet(p, inboundStreams, outboundStreams));
            }
        }

        final List<Tasklet> receivers = receiverMap
                .values().stream().flatMap(e -> e.values().stream()).collect(toList());

        tasklets.addAll(receivers);
    }

    private ConcurrentInboundEdgeStream getInboundEdgeStream(EdgeDef edge, ConcurrentConveyor<Object> conveyor) {
        final InboundEmitter[] emitters = new InboundEmitter[conveyor.queueCount()];
        Arrays.setAll(emitters, n -> new ConveyorEmitter(conveyor, n));
        return new ConcurrentInboundEdgeStream(emitters, edge.getOrdinal(), edge.getPriority());
    }

    private int getPartitionCount() {
        return nodeEngine.getPartitionService().getPartitionCount();
    }

    private OutboundEdgeStream getOutboundEdgeStream(VertexDef source, VertexDef destination,
                                                     EdgeDef edge, int taskletIndex,
                                                     ConcurrentConveyor<Object>[] conveyorArray) {
        // if a local edge, we will take all partitions, if not only partitions local to this node
        // and distribute them among the local consumers
        int localConsumerCount = destination.getParallelism();

        final Map<Integer, int[]> localPartitions = consumerToPartitions(localConsumerCount,
                edge.isDistributed());
        final int ordinalAtDestination = edge.getOtherEndOrdinal();

        final OutboundCollector[] localCollectors = new OutboundCollector[localConsumerCount];
        Arrays.setAll(localCollectors, n ->
                new ConveyorCollector(conveyorArray[n], taskletIndex, localPartitions.get(n)));

        final int partitionCount = getPartitionCount();
        final int parallelism = source.getParallelism();
        final int destinationId = edge.getOtherEndId();
        final OutboundCollector[] allCollectors;
        if (!edge.isDistributed()) {
            allCollectors = localCollectors;
        } else {
            // create the receiver tasklet for the edge, if not already created
            receiverMap.computeIfAbsent(destinationId, x -> new HashMap<>());
            receiverMap.get(destinationId).computeIfAbsent(ordinalAtDestination, x -> {
                final OutboundCollector[] receivers = new OutboundCollector[localConsumerCount];
                Arrays.setAll(receivers, n ->
                        new ConveyorCollector(conveyorArray[n], parallelism, localPartitions.get(n)));
                final OutboundCollector collector = compositeCollector(receivers, edge, partitionCount);
                final int senderCount = nodeEngine.getClusterService().getSize() - 1;
                return new ReceiverTasklet(
                        nodeEngine.getSerializationService(), collector, parallelism * senderCount);
            });
            // distribute remote partitions
            final Map<Address, int[]> remotePartitions = addrToPartitions();
            allCollectors = new OutboundCollector[remotePartitions.size() + 1];
            allCollectors[0] = compositeCollector(localCollectors, edge, partitionCount);
            int index = 1;
            for (Map.Entry<Address, int[]> entry : remotePartitions.entrySet()) {
                allCollectors[index++] = new RemoteOutboundCollector(
                        nodeEngine, context.getName(), entry.getKey(), plan.getId(), destinationId,
                        ordinalAtDestination, entry.getValue());
            }
        }
        return new OutboundEdgeStream(edge.getOrdinal(), compositeCollector(allCollectors, edge, partitionCount));
    }

    private void initializePartitioner(VertexDef vertex) {
        for (EdgeDef output : vertex.getOutputs()) {
            if (output.getPartitioner() != null) {
                output.getPartitioner().init(nodeEngine.getPartitionService());
            }
        }
    }

    private List<Processor> getProcessors(VertexDef vertexDef) {
        final ProcessorSupplier processorSupplier = vertexDef.getProcessorSupplier();
        int parallelism = vertexDef.getParallelism();
        processorSupplier.init(ProcessorSupplier.Context.of(nodeEngine.getHazelcastInstance(),
                parallelism));
        return processorSupplier.get(parallelism);
    }

    private Map<Address, int[]> addrToPartitions() {
        Address thisAddress = nodeEngine.getThisAddress();
        Map<Address, List<Integer>> partitionOwnerMap = nodeEngine.getPartitionService().getMemberPartitionsMap();
        final Map<Address, List<Integer>> addrToPartitions = partitionOwnerMap
                .entrySet().stream()
                .filter(e -> !e.getKey().equals(thisAddress))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        return toIntArrayMap(addrToPartitions);
    }

    private Map<Integer, int[]> consumerToPartitions(int localConsumerCount, boolean isEdgeDistributed) {
        Address thisAddress = nodeEngine.getThisAddress();
        IPartitionService ptionService = nodeEngine.getPartitionService();
        int partitionCount = ptionService.getPartitionCount();
        final Map<Integer, List<Integer>> consumerToPartitions = IntStream
                .range(0, partitionCount).boxed()
                .filter(p -> !isEdgeDistributed || ptionService.getPartitionOwnerOrWait(p).equals(thisAddress))
                .collect(groupingBy(p -> p % localConsumerCount));
        return toIntArrayMap(consumerToPartitions);
    }

    private static <K> Map<K, int[]> toIntArrayMap(Map<K, List<Integer>> intListMap) {
        return intListMap.entrySet().stream()
                         .collect(toMap(Map.Entry::getKey, e -> e.getValue().stream().mapToInt(x -> x).toArray()));
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentConveyor<Object>[] createConveyorArray(int count, int queueCount, int queueSize) {
        ConcurrentConveyor<Object>[] concurrentConveyors = new ConcurrentConveyor[count];
        Arrays.setAll(concurrentConveyors, i -> {
            QueuedPipe<Object>[] queues = new QueuedPipe[queueCount];
            Arrays.setAll(queues, j -> new OneToOneConcurrentArrayQueue<>(queueSize));
            return concurrentConveyor(DONE_ITEM, queues);
        });
        return concurrentConveyors;
    }
}
