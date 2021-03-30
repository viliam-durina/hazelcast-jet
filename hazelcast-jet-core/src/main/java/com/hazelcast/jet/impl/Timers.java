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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class Timers {

    private static Timers INSTANCE = new Timers();

    public final Timer executionPlan_initialize = new Timer("executionPlan_initialize");
    public final Timer submitCooperativeTasklets = new Timer("submitCooperativeTasklets");
    public final Timer submitLightJobOperation_run = new Timer("submitLightJobOperation_run");
    public final Timer noopPInitToClose = new Timer("noopPInitToClose");
    public final Timer jobExecService_runLightJob_synchronization_inner = new Timer("jobExecService_synchronization_inner");
    public final Timer jobExecService_runLightJob_synchronization_outer = new Timer("jobExecService_synchronization_outer");
    public final Timer jobExecService_runLightJob_verifyClusterInfo = new Timer("jobExecService_runLightJob_verifyClusterInfo");
    public final Timer initExecOp_deserializePlan = new Timer("initExecOp_deserializePlan");
    public final Timer execCtx_initialize = new Timer("execCtx_initialize");
    public final Timer lightMasterContext_start = new Timer("lightMasterContext_start");
    public final Timer execPlanBuilder_createPlans = new Timer("execPlanBuilder_createPlans");
    public final Timer execPlanBuilder_createOneInitExecutionOp = new Timer("execPlanBuilder_createOneInitExecutionOp");

    public static void resetAll() {
        INSTANCE = new Timers();
    }

    public void printAll() {
        executionPlan_initialize.print();
        submitCooperativeTasklets.print();
        submitLightJobOperation_run.print();
        noopPInitToClose.print();
        jobExecService_runLightJob_synchronization_inner.print();
        jobExecService_runLightJob_synchronization_outer.print();
        jobExecService_runLightJob_verifyClusterInfo.print();
        initExecOp_deserializePlan.print();
        execCtx_initialize.print();
        lightMasterContext_start.print();
        execPlanBuilder_createPlans.print();
        execPlanBuilder_createOneInitExecutionOp.print();
    }

    public static Timers i() {
        return INSTANCE;
    }

    public static final class Timer {
        private static final AtomicLongFieldUpdater<Timer> TOTAL_TIME_UPDATER =
                AtomicLongFieldUpdater.newUpdater(Timer.class, "totalTime");
        private static final AtomicIntegerFieldUpdater<Timer> RUN_COUNT_UPDATER =
                AtomicIntegerFieldUpdater.newUpdater(Timer.class, "runCount");

        private final String name;
        private volatile long totalTime;
        private volatile int runCount;

        public Timer(String name) {
            this.name = name;
        }

        public void start() {
            TOTAL_TIME_UPDATER.addAndGet(this, -System.nanoTime());
            RUN_COUNT_UPDATER.incrementAndGet(this);
        }

        public void stop() {
            TOTAL_TIME_UPDATER.addAndGet(this, System.nanoTime());
        }

        private void print() {
            System.out.println(name + ": " + NANOSECONDS.toMicros(totalTime / runCount) + " (" + runCount + ")");
        }
    }
}
