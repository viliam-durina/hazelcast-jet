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

package com.hazelcast.jet.impl.connector;

import com.google.common.collect.Lists;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.function.Observer;
import com.hazelcast.jet.impl.JetBlockHoundIntegration;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.map.IMap;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.blockhound.BlockHound;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertCollectedEventually;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JetBlockHoundTest extends SimpleTestInClusterSupport {

    private final Pipeline p = Pipeline.create();

    @BeforeClass
    public static void setUpClass() {
        BlockHound.builder()
                  .with(new JetBlockHoundIntegration())
                  .install();

        JetConfig config = new JetConfig();
        config.getHazelcastConfig().getMapConfig("journaled-*").getEventJournalConfig().setEnabled(true);
        initialize(2, config);
    }

    @Test
    public void test_testSources_longStream() {
        p.readFrom(TestSources.longStream(1, 0))
         .withoutTimestamps()
         .writeTo(Sinks.noop());

        Job job = instance().newJob(p);
        assertThatThrownBy(() -> job.getFuture().get(2, SECONDS))
                .isInstanceOf(TimeoutException.class);

    }

    @Test
    public void test_mapJournal() {
        String mapName = "journaled-" + randomName();
        p.readFrom(Sources.mapJournal(mapName, JournalInitialPosition.START_FROM_OLDEST))
         .withoutTimestamps()
         .writeTo(Sinks.noop());
        Job job = instance().newJob(p);
        assertJobStatusEventually(job, RUNNING);

        IMap<Integer, String> map = instance().getMap(mapName);
        for (int i = 0; i < 1000; i++) {
            map.put(i, "v");
            sleepMillis(1);
        }

        assertThatThrownBy(() -> job.getFuture().get(2, SECONDS))
                .isInstanceOf(TimeoutException.class);
    }

    @Test
    public void test_assertionSink() {
        p.readFrom(TestSources.longStream(1, 0))
         .withoutTimestamps()
         .writeTo(assertCollectedEventually(2, items -> {
             assertContainsAll(items, Arrays.asList(0L, 1L));
         }));

        Job job = instance().newJob(p);
        assertThatThrownBy(() -> job.getFuture().get(2, SECONDS))
                .isInstanceOf(TimeoutException.class);
    }

    @Test
    public void test_observableSink() {
        String observableName = "my-observable";
        Observable<Long> observable = instance().getObservable(observableName);
        observable.addObserver(new CollectingObserver());

        try {
            p.readFrom(TestSources.items(1L, 0L))
             .writeTo(Sinks.observable(observableName));

            instance().newJob(p).join();
            List<Long> items = new ArrayList<>();
            for (Long item : observable) {
                items.add(item);
            }
            assertContainsAll(items, Lists.newArrayList(0L, 1L));
        } finally {
            observable.destroy();
        }
    }

    @Test
    public void test_writeFiles() throws IOException {
        File tmpDir = createTempDirectory();
        p.readFrom(TestSources.items(1, 2, 3, 4))
         .writeTo(Sinks.files(tmpDir.toString()));

        instance().newJob(p).join();
    }

    @Test
    public void test_readMapP() {
        String mapName = randomName();
        instance().getMap(mapName).putAll(IntStream.range(0, 10_000).boxed().collect(toMap(i -> i, i -> i)));
        p.readFrom(Sources.map(mapName))
         .writeTo(Sinks.noop());

        instance().newJob(p).join();
    }

    @Test
    public void test_writeMapP() {
        p.readFrom(TestSources.items(IntStream.range(0, 10_000).boxed().collect(toList())))
         .map(i -> Util.entry(i, i))
         .writeTo(Sinks.map(randomName()));

        instance().newJob(p).join();
    }

    private static final class CollectingObserver implements Observer<Long> {

        private final List<Long> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void onNext(@Nonnull Long value) {
            values.add(value);
        }

        @Override
        public void onError(@Nonnull Throwable throwable) {
        }

        @Override
        public void onComplete() {
        }
    }
}
