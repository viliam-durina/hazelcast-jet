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

package com.hazelcast.jet.server;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.deployment.JetClassLoader;
import com.hazelcast.jet.impl.util.AsyncSnapshotWriterImpl.SnapshotDataKey;
import com.hazelcast.jet.impl.util.AsyncSnapshotWriterImpl.SnapshotDataValueTerminator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.quorum.QuorumService;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.SharedService;
import com.hazelcast.spi.impl.SerializationServiceSupport;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.transaction.TransactionManagerService;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.wan.WanReplicationService;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.Arrays.asList;

public class SnapshotViewerUtil {

    public static String getActiveSnapshotMap(JetInstance client, long jobId) {
        JobRepository jobRepository = new JobRepository(client);
        int mapIndex = jobRepository.getJobExecutionRecord(jobId).dataMapIndex();
        return JobRepository.snapshotDataMapName(jobId, mapIndex);
    }

    public static String getExportedSnapshotMap(String exportedSnapshotName) {
        return JobRepository.exportedSnapshotMapName(exportedSnapshotName);
    }

    public static List<String> getVertices(JetInstance client, String snapshotMap) {
        IMapJet<Object, byte[]> map = client.getMap(snapshotMap);
        // TODO rewrite with aggregator
        Set<String> vertices = new HashSet<>();
        for (Object key : map.keySet()) {
            if (key instanceof SnapshotDataKey) {
                vertices.add(((SnapshotDataKey) key).vertexName());
            }
        }
        String[] result = vertices.toArray(new String[0]);
        Arrays.sort(result);
        return asList(result);
    }

    public static JetClassLoader createJobClassLoader(JetInstance client, long jobId) {
        IMap<String, byte[]> resourcesMap =
                client.getMap(JobRepository.RESOURCES_MAP_NAME_PREFIX + idToString(jobId));
        JetClassLoader jetCl = new JetClassLoader(new MockNodeEngine(client.getHazelcastInstance()),
                SnapshotViewerUtil.class.getClassLoader(), idToString(jobId), jobId, () -> resourcesMap);
        return jetCl;
    }

    public static List<Tuple3<String, String, String>> getSnapshotEntries(
            JetInstance client, String snapshotMap, @Nullable ClassLoader classLoader
    ) {
        IMapJet<Object, byte[]> map = client.getMap(snapshotMap);
        InternalSerializationService serializationService =
                (InternalSerializationService) ((SerializationServiceSupport) client.getHazelcastInstance()).getSerializationService();
        List<Tuple3<String, String, String>> result = new ArrayList<>();
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader != null) {
            Thread.currentThread().setContextClassLoader(classLoader);
        }
        try {
            for (Entry<Object, byte[]> entry : map.entrySet()) {
                if (!(entry.getKey() instanceof SnapshotDataKey)) {
                    continue;
                }
                BufferObjectDataInput in = serializationService.createObjectDataInput(entry.getValue());
                for (;;) {
                    try {
                        Object key = in.readObject();
                        if (key == SnapshotDataValueTerminator.INSTANCE) {
                            in.close();
                            break;
                        }
                        if (key instanceof BroadcastKey) {
                            // unwrap the key
                            key = ((BroadcastKey) key).key();
                        }
                        Object value = in.readObject();
                        result.add(tuple3(((SnapshotDataKey) entry.getKey()).vertexName(), String.valueOf(key),
                                String.valueOf(value)));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            throw rethrow(e);
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
        result.sort(Comparator.<Tuple3<String, String, String>, String>comparing(Tuple3::f0)
                .thenComparing(Tuple3::f1));
        return result;
    }

    public static void main(String[] args) {
        JetInstance jet = Jet.newJetClient();
        Job job = jet.getJobs().iterator().next();

        try {
            System.out.println("list of vertices:");
            for (String vertex : getVertices(jet, getActiveSnapshotMap(jet, job.getId()))) {
                System.out.println(vertex);
            }
            System.out.println("list of vertices end.");

            System.out.println("snapshot data:");
            ClassLoader cl = SnapshotViewerUtil.createJobClassLoader(jet, job.getId());
            for (Tuple3<String, String, String> snapshotEntry :
                    getSnapshotEntries(jet, getActiveSnapshotMap(jet, job.getId()), cl)) {
                System.out.println(snapshotEntry);
            }
            System.out.println("snapshot data end");
        } finally {
            jet.shutdown();
        }
    }

    private static class MockNodeEngine implements NodeEngine {
        private final HazelcastInstance instance;

        private MockNodeEngine(HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public OperationService getOperationService() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExecutionService getExecutionService() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ClusterService getClusterService() {
            throw new UnsupportedOperationException();
        }

        @Override
        public IPartitionService getPartitionService() {
            throw new UnsupportedOperationException();
        }

        @Override
        public EventService getEventService() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SerializationService getSerializationService() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ProxyService getProxyService() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WanReplicationService getWanReplicationService() {
            throw new UnsupportedOperationException();
        }

        @Override
        public QuorumService getQuorumService() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TransactionManagerService getTransactionManagerService() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Address getMasterAddress() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Address getThisAddress() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Member getLocalMember() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Config getConfig() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ClassLoader getConfigClassLoader() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HazelcastProperties getProperties() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ILogger getLogger(String s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ILogger getLogger(Class aClass) {
            return Logger.getLogger(aClass);
        }

        @Override
        public Data toData(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T toObject(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T toObject(Object o, Class aClass) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isActive() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isRunning() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HazelcastInstance getHazelcastInstance() {
            return instance;
        }

        @Override
        public <T> T getService(String s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T extends SharedService> T getSharedService(String s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MemberVersion getVersion() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SplitBrainMergePolicyProvider getSplitBrainMergePolicyProvider() {
            throw new UnsupportedOperationException();
        }

        @Override
        public <S> Collection<S> getServices(Class<S> aClass) {
            throw new UnsupportedOperationException();
        }
    }
}
