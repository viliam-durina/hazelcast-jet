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

package com.hazelcast.jet.sql;

import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.connector.LocalPartitionedMapConnector;
import com.hazelcast.sql.impl.connector.LocalReplicatedMapConnector;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.jet.impl.util.Util.toList;
import static java.lang.String.format;
import static java.util.Collections.emptyList;

public class ImdgPlanIntegrationTest extends SqlTestSupport {

    private static final String INT_TO_STRING_MAP_SRC = "int_to_string_map_src";
    private static final String INT_TO_STRING_R_MAP_SRC = "int_to_string_r_map_src";

    @BeforeClass
    public static void beforeClass() {
        executeSql(
                format("CREATE EXTERNAL TABLE %s (__key INT, this VARCHAR) TYPE \"%s\"",
                        INT_TO_STRING_MAP_SRC, LocalPartitionedMapConnector.TYPE_NAME)
        );
        executeSql(
                format("CREATE EXTERNAL TABLE %s (__key INT, this VARCHAR) TYPE \"%s\"",
                        INT_TO_STRING_R_MAP_SRC, LocalReplicatedMapConnector.TYPE_NAME)
        );
    }

    @Test
    public void select_empty() {
        assertRowsEventuallyAnyOrder("SELECT /*+ jet */ * FROM " + INT_TO_STRING_MAP_SRC,
                emptyList());
    }

    @Test
    public void select_large() {
        IMap<Integer, String> intToStringMap = instance().getMap(INT_TO_STRING_MAP_SRC);
        Map<Integer, String> items = new HashMap<>(16384);
        for (int i = 0; i < 16384; i++) {
            items.put(i, "value-" + i);
        }
        intToStringMap.putAll(items);

        assertRowsEventuallyAnyOrder("SELECT /*+ jet */ * FROM " + INT_TO_STRING_MAP_SRC,
                toList(items.entrySet(), en -> new Row(en.getKey(), en.getValue())));
    }

    @Test
    public void select_replicated_large() {
        IMap<Integer, String> intToStringMap = instance().getMap(INT_TO_STRING_MAP_SRC);
        Map<Integer, String> items = new HashMap<>(16384);
        for (int i = 0; i < 16384; i++) {
            items.put(i, "value-" + i);
        }
        intToStringMap.putAll(items);

        assertRowsEventuallyAnyOrder("SELECT /*+ jet */ * FROM " + INT_TO_STRING_MAP_SRC,
                toList(items.entrySet(), en -> new Row(en.getKey(), en.getValue())));
    }

    @Test
    public void test_tumble_REMOVEME() {
        executeQuery("select * from TABLE(TUMBLE(TABLE int_to_string_map_src, DESCRIPTOR(__key), interval '1' minute))");
    }
}
