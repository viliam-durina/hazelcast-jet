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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.TestProcessors.ListSource;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.Processors.noopP;
import static com.hazelcast.test.PacketFiltersUtil.delayOperationsFrom;
import static java.util.Collections.singletonList;

@RunWith(HazelcastSerialClassRunner.class)
public class ExecutionContextTest extends JetTestSupport {

    @Test
    public void test_packetsReceivedBeforeExecutionContextExists() {
        JetInstance inst1 = createJetMember();
        JetInstance inst2 = createJetMember();

        delayOperationsFrom(hz(inst1), JetInitDataSerializerHook.FACTORY_ID,
                singletonList(JetInitDataSerializerHook.START_EXECUTION_OP));

        DAG dag = new DAG();
        Vertex src = dag.newVertex("src", ListSource.supplier(singletonList("item")));
        Vertex sink = dag.newVertex("sink", noopP());
        dag.edge(between(src, sink).distributed());

        inst1.newJob(dag).join();
    }
}
