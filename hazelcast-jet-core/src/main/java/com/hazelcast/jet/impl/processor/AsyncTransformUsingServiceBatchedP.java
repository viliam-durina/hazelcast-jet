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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.pipeline.ServiceFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.processor.ProcessorSupplierWithService.supplierWithService;

/**
 * Processor which, for each received item, emits all the items from the
 * traverser returned by the given async item-to-traverser function, using a
 * service.
 * <p>
 * This processor keeps the order of input items: a stalling call for one item
 * will stall all subsequent items.
 *
 * @param <S> context object type
 * @param <T> received item type
 * @param <R> emitted item type
 */
public final class AsyncTransformUsingServiceBatchedP<C, S, T, R>
        extends AsyncTransformUsingServiceOrderedP<C, S, List<T>, R> {

    private final int maxBatchSize;
    private List<T> currentBatch;
    private int currentBatchOrdinal;
    private int tryProcessWithoutProcessCount;
    private Watermark withheldWatermark;

    /**
     * Constructs a processor with the given mapping function.
     */
    private AsyncTransformUsingServiceBatchedP(
            @Nonnull ServiceFactory<C, S> serviceFactory,
            @Nonnull C serviceContext,
            int maxConcurrentOps,
            int maxBatchSize,
            @Nonnull BiFunctionEx<? super S, ? super List<T>, ? extends CompletableFuture<Traverser<R>>> callAsyncFn
    ) {
        super(serviceFactory, serviceContext, maxConcurrentOps, callAsyncFn);
        this.maxBatchSize = maxBatchSize;
        this.currentBatch = new ArrayList<>(maxBatchSize);
    }

    @Override
    public boolean tryProcess() {
        if (tryProcessWithoutProcessCount++ == 2) {
            sendBatch();
        }
        return super.tryProcess();
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        tryProcessWithoutProcessCount = 0;
        if (isQueueFull() && !tryFlushQueue()) {
            return;
        }
        if (ordinal != currentBatchOrdinal) {
            sendBatch();
            currentBatchOrdinal = ordinal;
            assert currentBatch.isEmpty() : "current batch not empty";
        }
        inbox.drainTo(currentBatch, maxBatchSize - currentBatch.size());
        assert currentBatch.size() <= maxBatchSize : "currentBatch.size=" + currentBatch.size()
                + ", maxBatchSize=" + maxBatchSize;
        if (currentBatch.size() == maxBatchSize) {
            sendBatch();
        }
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        if (currentBatch.isEmpty()) {
            return super.tryProcessWatermark(watermark);
        }
        withheldWatermark = watermark;
        return true;
    }

    @Override
    public boolean complete() {
        if (isQueueFull() && !tryFlushQueue()) {
            return false;
        }
        sendBatch();
        return super.complete();
    }

    private void sendBatch() {
        if (currentBatch.isEmpty()) {
            return;
        }
        assert !isQueueFull() : "sendBatch() called with full queue";
        boolean success = super.tryProcess(currentBatchOrdinal, currentBatch);
        assert success : "the superclass didn't handle the batch";
        currentBatch = new ArrayList<>(maxBatchSize);
        if (withheldWatermark != null) {
            success = super.tryProcessWatermark(withheldWatermark);
            assert success : "the superclass didn't handle the watermark";
            withheldWatermark = null;
        }
    }

    /**
     * The {@link ResettableSingletonTraverser} is passed as a first argument to
     * {@code callAsyncFn}, it can be used if needed.
     */
    public static <C, S, T, R> ProcessorSupplier supplier(
            @Nonnull ServiceFactory<C, S> serviceFactory,
            int maxConcurrentOps,
            int maxBatchSize,
            @Nonnull BiFunctionEx<? super S, ? super List<T>, ? extends CompletableFuture<Traverser<R>>> callAsyncFn
    ) {
        return supplierWithService(serviceFactory, (factory, context) ->
                new AsyncTransformUsingServiceBatchedP<>(factory, context, maxConcurrentOps, maxBatchSize, callAsyncFn));
    }
}
