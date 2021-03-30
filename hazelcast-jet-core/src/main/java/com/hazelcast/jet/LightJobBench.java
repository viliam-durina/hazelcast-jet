/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;

import java.io.IOException;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class LightJobBench {

    private static int warmUpIterations;
    private static int measuredIterations;

    private static JetInstance jetInst;

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage:");
            System.err.println("  LightJobBench <jet|imdg> <warmUpIterations> <measuredIterations>");
            System.exit(1);
        }

        boolean isJet = "jet".equalsIgnoreCase(args[0]);
        warmUpIterations = Integer.parseInt(args[1]);
        measuredIterations = Integer.parseInt(args[2]);

        jetInst = Jet.newJetInstance();

        if (isJet) {
            jetBench();
        } else {
            sqlBench();
        }

        Jet.shutdownAll();
    }

    public static void jetBench() throws IOException {
        DAG dag = new DAG();
        dag.newVertex("v", Processors.noopP());
        System.out.println("will submit " + warmUpIterations + " jobs");
        for (int i = 0; i < warmUpIterations; i++) {
            jetInst.newLightJob(dag).join();
        }
        System.out.println("warmup jobs done");
        System.out.println("attach profiler and press enter");
        System.in.read();
        System.out.println("starting benchmark");
        long start = System.nanoTime();
        for (int i = 0; i < measuredIterations; i++) {
            jetInst.newLightJob(dag).join();
        }
        long elapsedMicros = NANOSECONDS.toMicros(System.nanoTime() - start);
        System.out.println(measuredIterations + " jobs run in " + (elapsedMicros / measuredIterations) + " us/job");
    }

    public static void sqlBench() {
        SqlService sqlService = jetInst.getSql();
        System.out.println("will submit " + warmUpIterations + " jobs");
        jetInst.getMap("m").put(1, 1);
        int numRows = 0;
        for (int i = 0; i < warmUpIterations; i++) {
            for (SqlRow r : sqlService.execute("select * from m")) {
                numRows++;
            }
        }
        System.out.println("warmup jobs done, starting benchmark");
        long start = System.nanoTime();
        for (int i = 0; i < measuredIterations; i++) {
            for (SqlRow r : sqlService.execute("select * from m")) {
                numRows++;
            }
        }
        long elapsedMicros = NANOSECONDS.toMicros(System.nanoTime() - start);
        System.out.println(numRows);
        System.out.println(measuredIterations + " queries run in " + (elapsedMicros / measuredIterations) + " us/job");
    }
}