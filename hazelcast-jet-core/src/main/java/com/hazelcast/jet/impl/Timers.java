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

import com.hazelcast.jet.impl.operation.LightMasterContext;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class Timers {

    private static Timers INSTANCE = new Timers();

    public final Timer executionPlan_initialize = new Timer("executionPlan_initialize");
    public final Timer submitCooperativeTasklets = new Timer("submitCooperativeTasklets");
    public final Timer submitLightJobOperation_run;

    public static void resetAll() {
        INSTANCE = new Timers();
    }

    public void printAll() {
        executionPlan_initialize.print();
        submitCooperativeTasklets.print();
    }

    public static Timers i() {
        return INSTANCE;
    }

    public static final class Timer {
        private final String name;
        private long totalTime;
        private int runCount;

        public Timer(String name) {
            this.name = name;
        }

        public void start() {
            totalTime -= System.nanoTime();
            runCount++;
        }

        public void stop() {
            totalTime += System.nanoTime();
        }

        private void print() {
            System.out.println(name + ": " + NANOSECONDS.toMicros(totalTime / runCount) + " (" + runCount + ")");
        }
    }
}
