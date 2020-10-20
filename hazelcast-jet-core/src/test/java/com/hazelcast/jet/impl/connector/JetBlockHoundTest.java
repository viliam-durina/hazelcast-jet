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

import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.impl.JetBlockHoundIntegration;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.blockhound.BlockHound;

import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JetBlockHoundTest extends SimpleTestInClusterSupport {

    @BeforeClass
    public static void setUpClass() {
        BlockHound.builder()
                  .with(new JetBlockHoundIntegration())
                  .install();

        initialize(2, null);
    }

    @Test
    public void test() {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.longStream(1, 0))
         .withoutTimestamps()
         .writeTo(Sinks.noop());

        Job job = instance().newJob(p);
        assertThatThrownBy(() -> job.getFuture().get(2, SECONDS))
                .isInstanceOf(TimeoutException.class);

    }
}
