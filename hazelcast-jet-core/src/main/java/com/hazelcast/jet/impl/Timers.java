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

package com.hazelcast.jet.impl;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class Timers {

    private static Timers INSTANCE = new Timers();

    private final List<Timer> allTimers = new ArrayList<>();

    public final Timer executionPlan_initialize = add("executionPlan_initialize");
    public final Timer submitCooperativeTasklets = add("submitCooperativeTasklets");
    public final Timer submitLightJobOperation_run = add("submitLightJobOperation_run");
    public final Timer noopPInitToClose = add("noopPInitToClose");
    public final Timer jobExecService_runLightJob_synchronization_inner = add("jobExecService_synchronization_inner");
    public final Timer jobExecService_runLightJob_synchronization_outer = add("jobExecService_synchronization_outer");
    public final Timer jobExecService_runLightJob_verifyClusterInfo = add("jobExecService_runLightJob_verifyClusterInfo");
    public final Timer jobExecService_completeExecution = add("jobExecService_completeExecution");
    public final Timer initExecOp_deserializePlan = add("initExecOp_deserializePlan");
    public final Timer execCtx_initialize = add("execCtx_initialize");
    public final Timer lightMasterContext_start = add("lightMasterContext_start");
    public final Timer execPlanBuilder_createPlans = add("execPlanBuilder_createPlans");
    public final Timer lightMasterContext_serializeOnePlan = add("lightMasterContext_serializeOnePlan");
    public final Timer initResponseTime = add("initResponseTime");
    public final Timer init = add("init");
    public final Timer processorClose = add("processorClose");

    private long globalStart = System.nanoTime();

    public static void reset() {
        INSTANCE = new Timers();
    }

    public void setGlobalStart() {
        globalStart = System.nanoTime();
    }

    private Timer add(String name) {
        Timer t = new Timer(name);
        allTimers.add(t);
        return t;
    }

    public void printAll() {
        System.out.println("-- sorted by start");
        allTimers.sort(Comparator.comparing(t -> t.totalTimeToStart));
        for (Timer t : allTimers) {
            t.print();
        }
        System.out.println("-- sorted by end");
        allTimers.sort(Comparator.comparing(t -> t.totalTimeToEnd));
        for (Timer t : allTimers) {
            t.print();
        }
    }

    public static Timers i() {
        return INSTANCE;
    }

    private static final AtomicLongFieldUpdater<Timer> TOTAL_TIME_TO_START_UPDATER =
            AtomicLongFieldUpdater.newUpdater(Timer.class, "totalTimeToStart");
    private static final AtomicLongFieldUpdater<Timer> TOTAL_TIME_TO_END_UPDATER =
            AtomicLongFieldUpdater.newUpdater(Timer.class, "totalTimeToEnd");
    private static final AtomicIntegerFieldUpdater<Timer> RUN_COUNT_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(Timer.class, "runCount");

    public final class Timer {
        private final String name;
        volatile long totalTimeToStart;
        volatile long totalTimeToEnd;
        volatile int runCount;

        public Timer(String name) {
            this.name = name;
        }

        public void start() {
            TOTAL_TIME_TO_START_UPDATER.addAndGet(this, System.nanoTime() - globalStart);
            RUN_COUNT_UPDATER.incrementAndGet(this);
        }

        public void stop() {
            TOTAL_TIME_TO_END_UPDATER.addAndGet(this, System.nanoTime() - globalStart);
        }

        private void print() {
            System.out.printf("%50s: toStart: %8s, toEnd: %8s, dur: %8s, runCount: %d\n",
                    name,
                    runCount != 0 ? NANOSECONDS.toMicros(totalTimeToStart / runCount) : "--",
                    runCount != 0 ? NANOSECONDS.toMicros(totalTimeToEnd / runCount) : "--",
                    runCount != 0 ? NANOSECONDS.toMicros((totalTimeToEnd - totalTimeToStart) / runCount) : "--",
                    runCount);
        }
    }
}
