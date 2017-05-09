/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.client.time.CurrentTimeEvent;
import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorSupplier;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.hazelcast.util.Preconditions.checkPositive;
import static java.util.stream.Collectors.toList;

public class EsperP extends AbstractProcessor {

    private EPStatement[] statements;
    private EPServiceProvider epService;

    public EsperP(String[] queries, List<String> eventTypeAutoNamePackages) {
        checkPositive(queries.length, "At least one statement required");

        Configuration config = new Configuration();
        // we manage threads ourselves
        config.getEngineDefaults().getExecution().setDisableLocking(true);
        config.getEngineDefaults().getThreading().setInternalTimerEnabled(false);
        config.getEngineDefaults().getThreading().setThreadPoolInbound(false);
        config.getEngineDefaults().getThreading().setThreadPoolOutbound(false);
        config.getEngineDefaults().getThreading().setThreadPoolRouteExec(false);
        for (String packageName : eventTypeAutoNamePackages) {
            config.addEventTypeAutoName(packageName);
        }
        epService = EPServiceProviderManager.getProvider(UUID.randomUUID().toString(), config);

        statements = new EPStatement[queries.length];
        for (int index = 0; index < queries.length; index++) {
            String query = queries[index];
            statements[index] = epService.getEPAdministrator().createEPL(query);
            statements[index].addListener(new EsperListener(index));
        }
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        epService.getEPRuntime().sendEvent(item);
        return true;
    }

    @Override
    public boolean tryProcess() {
        // send timer event
        epService.getEPRuntime().sendEvent(new CurrentTimeEvent(System.currentTimeMillis()));
        return true;
    }

    public static class EsperUpdate {
        public final EventBean[] newEvents;
        public final EventBean[] oldEvents;

        public EsperUpdate(EventBean[] newEvents, EventBean[] oldEvents) {
            this.newEvents = newEvents;
            this.oldEvents = oldEvents;
        }
    }

    public static class EsperProcessorSupplier implements ProcessorSupplier {
        private final String[] queries;
        private final transient List<String> eventTypeAutoNamePackages = new ArrayList<>();
        private transient List<EsperP> processors;

        public EsperProcessorSupplier(String... queries) {
            this.queries = queries;
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            if (processors != null) {
                throw new IllegalStateException();
            }
            return processors = IntStream.range(0, count).mapToObj(i -> new EsperP(queries, eventTypeAutoNamePackages))
                    .collect(toList());
        }

        @Override
        public void complete(Throwable error) {
            for (EsperP processor : processors) {
                // release, if the early release did not happen (e.g. job was cancelled)
                processor.destroy();
            }
        }

        /**
         * See {@link Configuration#addEventTypeAutoName(String)}
         * @return {@code this} for chaining
         */
        public EsperProcessorSupplier addEventTypeAutoName(String packageName) {
            eventTypeAutoNamePackages.add(packageName);
            return this;
        }
    }

    private final class EsperListener implements UpdateListener {
        private final int index;

        EsperListener(int index) {
            this.index = index;
        }

        @Override
        public void update(EventBean[] newEvents, EventBean[] oldEvents) {
            EsperUpdate outputItem = new EsperUpdate(newEvents, oldEvents);
            if (!tryEmit(index, outputItem)) {
                throw new RuntimeException(EsperP.class.getSimpleName() + " should be configured with unbounded outbox");
            }
        }
    }

    @Override
    public boolean complete() {
        // early release
        destroy();
        return true;
    }

    private void destroy() {
        if (statements == null) {
            // already destroyed
            return;
        }

        for (EPStatement statement : statements) {
            statement.destroy();
        }
        epService.destroy();

        statements = null;
        epService = null;
    }

    public static EsperProcessorSupplier supplier(String ... queries) {
        return new EsperProcessorSupplier(queries);
    }
}
