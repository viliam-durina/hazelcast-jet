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
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JetConfig;
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

import java.util.concurrent.TimeoutException;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertCollectedEventually;
import static java.util.concurrent.TimeUnit.SECONDS;
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
    public void testAssertionsP() {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.longStream(1, 0))
         .withoutTimestamps()
         .writeTo(assertCollectedEventually(2, c -> {
             assertContainsAll(c, Lists.newArrayList(1L, 0L));
         }));

        Job job = instance().newJob(p);
        assertThatThrownBy(() -> job.getFuture().get(2, SECONDS))
                .isInstanceOf(TimeoutException.class);

    }
}
