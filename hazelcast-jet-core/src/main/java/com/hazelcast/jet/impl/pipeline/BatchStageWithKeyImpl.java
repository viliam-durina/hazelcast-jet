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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.function.BiPredicateEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.function.TriPredicate;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.jet.impl.pipeline.transform.DistinctTransform;
import com.hazelcast.jet.impl.pipeline.transform.GroupTransform;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.BatchStageWithKey;
import com.hazelcast.jet.pipeline.ServiceFactory;

import javax.annotation.Nonnull;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.JetEvent.jetEvent;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class BatchStageWithKeyImpl<T, K> extends StageWithGroupingBase<T, K> implements BatchStageWithKey<T, K> {

    BatchStageWithKeyImpl(
            @Nonnull BatchStageImpl<T> computeStage,
            @Nonnull FunctionEx<? super T, ? extends K> keyFn
    ) {
        super(computeStage, keyFn);
    }

    // TODO [viliam] adapt most of the functions here

    @Nonnull @Override
    public <S, R> BatchStage<R> mapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends R> mapFn
    ) {
        return attachMapStateful(0, createFn, mapFn, null);
    }

    @Nonnull @Override
    public <S> BatchStage<T> filterStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    ) {
        return attachMapStateful(0, createFn, (s, k, t) -> filterFn.test(s, t) ? t : null, null);
    }

    @Nonnull @Override
    public <S, R> BatchStage<R> flatMapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return attachFlatMapStateful(0, createFn, flatMapFn, null);
    }

    @Nonnull @Override
    public BatchStage<T> distinct() {
        FunctionEx<?, ? extends K> adaptedKeyFn = computeStage.fnAdapter.adaptKeyFn(keyFn());
        return computeStage.attach(new DistinctTransform<>(computeStage.transform, adaptedKeyFn), computeStage.fnAdapter);
    }

    @Nonnull @Override
    public <S, R> BatchStage<R> mapUsingService(
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends R> mapFn
    ) {
        return attachMapUsingService(serviceFactory, mapFn);
    }

    @Nonnull @Override
    public <S, R> BatchStage<R> mapUsingServiceAsync(
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull TriFunction<? super S, ? super K, ? super T, CompletableFuture<R>> mapAsyncFn
    ) {
        return attachTransformUsingServiceAsync("map", serviceFactory,
                (s, k, t) -> mapAsyncFn.apply(s, k, t).thenApply(Traversers::singleton));
    }

    @Nonnull @Override
    public <S> BatchStage<T> filterUsingService(
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull TriPredicate<? super S, ? super K, ? super T> filterFn
    ) {
        return attachFilterUsingService(serviceFactory, filterFn);
    }

    @Nonnull @Override
    public <S> BatchStage<T> filterUsingServiceAsync(
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull TriFunction<? super S, ? super K, ? super T, CompletableFuture<Boolean>>
                    filterAsyncFn
    ) {
        return attachTransformUsingServiceAsync("filter", serviceFactory,
                (s, k, t) -> filterAsyncFn.apply(s, k, t).thenApply(passed -> passed ? Traversers.singleton(t) : null));
    }

    @Nonnull @Override
    public <S, R> BatchStage<R> flatMapUsingService(
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return attachFlatMapUsingService(serviceFactory, flatMapFn);
    }

    @Nonnull @Override
    public <S, R> BatchStage<R> flatMapUsingServiceAsync(
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull TriFunction<? super S, ? super K, ? super T, CompletableFuture<Traverser<R>>>
                    flatMapAsyncFn
    ) {
        return attachTransformUsingServiceAsync("flatMap", serviceFactory, flatMapAsyncFn);
    }

    @Nonnull @Override
    public <R> BatchStage<R> customTransform(@Nonnull String stageName, @Nonnull ProcessorMetaSupplier procSupplier) {
        return computeStage.attachPartitionedCustomTransform(stageName, procSupplier, keyFn());
    }

    @Nonnull @Override
    public <R> BatchStage<Entry<K, R>> aggregate(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    ) {
        return computeStage.attach(new GroupTransform<>(
                singletonList(computeStage.transform),
                singletonList(adaptedKeyFn()),
                computeStage.fnAdapter.adaptAggregateOperation(aggrOp),
                (K k, R v) -> jetEvent(Util.entry(k, v), k, JetEvent.NO_TIMESTAMP)));
    }

    @Nonnull @Override
    public <T1, R> BatchStage<Entry<K, R>> aggregate2(
            @Nonnull BatchStageWithKey<T1, ? extends K> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, ?, R> aggrOp
    ) {
        @SuppressWarnings("unchecked")
        StageWithGroupingBase<T1, K> stage1Casted = (StageWithGroupingBase<T1, K>) stage1;
        return computeStage.attach(
                new GroupTransform<>(
                        asList(computeStage.transform, transformOf(stage1)),
                        asList(adaptedKeyFn(), stage1Casted.adaptedKeyFn()),
                        computeStage.fnAdapter.adaptAggregateOperation(aggrOp),
                        (K k, R v) -> jetEvent(Util.entry(k, v), k, JetEvent.NO_TIMESTAMP)));
    }

    @Nonnull @Override
    public <T1, T2, R> BatchStage<Entry<K, R>> aggregate3(
            @Nonnull BatchStageWithKey<T1, ? extends K> stage1,
            @Nonnull BatchStageWithKey<T2, ? extends K> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, ?, ? extends R> aggrOp
    ) {
        @SuppressWarnings("unchecked")
        StageWithGroupingBase<T1, K> stage1Casted = (StageWithGroupingBase<T1, K>) stage1;
        @SuppressWarnings("unchecked")
        StageWithGroupingBase<T2, K> stage2Casted = (StageWithGroupingBase<T2, K>) stage2;
        return computeStage.attach(
                new GroupTransform<>(
                        asList(computeStage.transform, transformOf(stage1), transformOf(stage2)),
                        asList(
                                adaptedKeyFn(),
                                stage1Casted.adaptedKeyFn(),
                                stage2Casted.adaptedKeyFn()),
                        computeStage.fnAdapter.adaptAggregateOperation(aggrOp),
                        (K k, R v) -> jetEvent(Util.entry(k, v), k, JetEvent.NO_TIMESTAMP)));
    }
}
