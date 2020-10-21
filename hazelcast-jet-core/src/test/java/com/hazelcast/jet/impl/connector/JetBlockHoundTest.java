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
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.config.JetClientConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.function.Observer;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.rabbitmq.jms.admin.RMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.junit.EmbeddedActiveMQResource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.RabbitMQContainer;
import reactor.blockhound.BlockHound;

import javax.annotation.Nonnull;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertCollectedEventually;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JetBlockHoundTest extends SimpleTestInClusterSupport {

    private static JetInstance remoteMember;
    private static JetClientConfig clientConfig;
    private static JetInstance remoteClient;

    private final Pipeline p = Pipeline.create();

    @BeforeClass
    public static void setUpClass() {
        BlockHound.install();

        JetConfig config = new JetConfig();
        config.getHazelcastConfig().getMapConfig("journaled-*").getEventJournalConfig().setEnabled(true);
        initializeWithClient(2, config, null);

        JetConfig config2 = new JetConfig();
        config2.getHazelcastConfig().getMapConfig("journaled-*").getEventJournalConfig().setEnabled(true);
        config2.getHazelcastConfig().setClusterName(randomName());
        remoteMember = Jet.newJetInstance(config2);

        clientConfig = new JetClientConfig();
        clientConfig.setClusterName(config2.getHazelcastConfig().getClusterName());

        remoteClient = Jet.newJetClient(clientConfig);
    }

    @AfterClass
    public static void afterClass() {
        remoteMember.getHazelcastInstance().getLifecycleService().terminate();
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
    public void test_mapJournal_local() {
        test_mapJournal(false);
    }

    @Test
    public void test_mapJournal_remote() {
        test_mapJournal(true);
    }

    private void test_mapJournal(boolean remote) {
        String mapName = "journaled-" + randomName();
        p.readFrom(remote
                ? Sources.remoteMapJournal(mapName, clientConfig, JournalInitialPosition.START_FROM_OLDEST)
                : Sources.mapJournal(mapName, JournalInitialPosition.START_FROM_OLDEST))
         .withoutTimestamps()
         .writeTo(Sinks.noop());
        Job job = instance().newJob(p);
        assertJobStatusEventually(job, RUNNING);

        IMap<Integer, String> map = (remote ? remoteClient : instance()).getMap(mapName);
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
         .writeTo(assertCollectedEventually(2, items -> assertContainsAll(items, Arrays.asList(0L, 1L))));

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
    public void test_readMapP_local() {
        test_readMapP(false);
    }

    @Test
    public void test_readMapP_remote() {
        test_readMapP(true);
    }

    private void test_readMapP(boolean remote) {
        String mapName = randomName();
        (remote ? remoteClient : instance()).getMap(mapName).putAll(IntStream.range(0, 10_000).boxed().collect(toMap(i -> i, i -> i)));
        p.readFrom(remote
                ? Sources.remoteMap(mapName, clientConfig)
                : Sources.map(mapName))
         .writeTo(Sinks.noop());

        instance().newJob(p).join();
    }

    @Test
    public void test_writeMapP_local() {
        test_writeMapP(false);
    }

    @Test
    public void test_writeMapP_remote() {
        test_writeMapP(true);
    }

    private void test_writeMapP(boolean remote) {
        p.readFrom(TestSources.items(IntStream.range(0, 10_000).boxed().collect(toList())))
         .map(i -> Util.entry(i, i))
         .writeTo(remote
                 ? Sinks.remoteMap(randomName(), clientConfig)
                 : Sinks.map(randomName()));

        instance().newJob(p).join();
    }

    @Test
    public void test_mapWithUpdating_local() {
        test_mapWithUpdating(false);
    }

    @Test
    public void test_mapWithUpdating_remote() {
        test_mapWithUpdating(true);
    }

    private void test_mapWithUpdating(boolean remote) {
        p.readFrom(TestSources.items(IntStream.range(0, 10_000).boxed().collect(toList())))
         .writeTo(remote
                 ? Sinks.<Integer, Integer, Integer>remoteMapWithUpdating(randomName(), clientConfig, i -> i % 100,
                 (v, item) -> v == null ? 1 : v + 1)
                 : Sinks.<Integer, Integer, Integer>mapWithUpdating(randomName(), i -> i % 100,
                 (v, item) -> v == null ? 1 : v + 1));

        instance().newJob(p).join();
    }

    @Test
    public void test_mapWithEntryProcessor_local() {
        test_mapWithEntryProcessor(false);
    }

    @Test
    public void test_mapWithEntryProcessor_remote() {
        test_mapWithEntryProcessor(true);
    }

    private void test_mapWithEntryProcessor(boolean remote) {
        p.readFrom(TestSources.items(IntStream.range(0, 10_000).boxed().collect(toList())))
         .writeTo(remote
                 ? Sinks.remoteMapWithEntryProcessor(randomName(), clientConfig, i -> i % 100,
                 (item) -> new IncrementByOneEntryProcessor())
                 : Sinks.mapWithEntryProcessor(randomName(), i -> i % 100,
                 (item) -> new IncrementByOneEntryProcessor()));

        instance().newJob(p).join();
    }

    @Test
    public void test_distributedEdge() {
        p.readFrom(TestSources.items(IntStream.range(0, 10_000).boxed().collect(toList())))
         .rebalance()
         .writeTo(Sinks.noop());

        instance().newJob(p).join();
    }

    @Test
    public void test_jmsSource_ActiveMq() throws JMSException {
        EmbeddedActiveMQResource broker = new EmbeddedActiveMQResource();
        broker.start();

        try {
            String url = broker.getVmURL();

            SupplierEx<? extends ConnectionFactory> connectionFactorySupplier = () -> new ActiveMQConnectionFactory(url);
            String queueName = "queue";

            StreamSource<String> source = Sources.jmsQueueBuilder(connectionFactorySupplier)
                                                 .maxGuarantee(ProcessingGuarantee.NONE) // does not make a difference
                                                 .connectionFn(ConnectionFactory::createConnection)
                                                 .consumerFn(session -> session.createConsumer(session.createQueue(queueName)))
                                                 .messageIdFn(m -> {
                                                     throw new UnsupportedOperationException();
                                                 })
                                                 .build(message -> ((TextMessage) message).getText());

            p.readFrom(source)
             .withoutTimestamps()
             .writeTo(assertCollectedEventually(2, items -> assertContainsAll(items, Arrays.asList("msg-0", "msg-1"))));

            JmsTestUtil.sendMessages(connectionFactorySupplier.get(), queueName, true, 2);

            Job job = instance().newJob(p);
            assertThatThrownBy(() -> job.getFuture().get(2, SECONDS))
                    .isInstanceOf(TimeoutException.class);
        } finally {
            broker.stop();
        }
    }

    @Test
    public void test_jmsSource_RabbitMq() throws JMSException {
        RabbitMQContainer broker = new RabbitMQContainer("rabbitmq:3.8");
        broker.start();

        try {
            String url = broker.getAmqpUrl();

            SupplierEx<? extends ConnectionFactory> connectionFactorySupplier = () -> {
                RMQConnectionFactory factory = new RMQConnectionFactory();
                factory.setUri(url);
                return factory;
            };
            String queueName = "queue";

            StreamSource<String> source = Sources.jmsQueueBuilder(connectionFactorySupplier)
                                                 .maxGuarantee(ProcessingGuarantee.NONE) // does not make a difference
                                                 .connectionFn(ConnectionFactory::createConnection)
                                                 .consumerFn(session -> session.createConsumer(session.createQueue(queueName)))
                                                 .messageIdFn(m -> {
                                                     throw new UnsupportedOperationException();
                                                 })
                                                 .build(message -> ((TextMessage) message).getText());

            p.readFrom(source)
             .withoutTimestamps()
             .writeTo(assertCollectedEventually(2, items -> assertContainsAll(items, Arrays.asList("msg-0", "msg-1"))));

            JmsTestUtil.sendMessages(connectionFactorySupplier.get(), queueName, true, 2);

            Job job = instance().newJob(p);
            assertThatThrownBy(() -> job.getFuture().get(2, SECONDS))
                    .isInstanceOf(TimeoutException.class);
        } finally {
            broker.stop();
        }
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

    private static class IncrementByOneEntryProcessor implements EntryProcessor<Integer, Integer, Void> {
        @Override
        public Void process(Entry<Integer, Integer> entry) {
            int newValue = entry.getValue() == null ? 1 : entry.getValue() + 1;
            entry.setValue(newValue);
            return null;
        }
    }
}
