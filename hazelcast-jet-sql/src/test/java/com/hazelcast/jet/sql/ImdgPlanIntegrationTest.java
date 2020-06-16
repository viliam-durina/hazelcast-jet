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

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.sql.impl.connector.LocalPartitionedMapConnector;
import com.hazelcast.sql.impl.connector.LocalReplicatedMapConnector;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.jet.impl.util.Util.toList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class ImdgPlanIntegrationTest extends SqlTestSupport {

    private static final String TEST_IMAP = "test_imap";
    private static final String TEST_R_MAP = "test_r_map";

    @BeforeClass
    public static void beforeClass() {
        JetConfig jetConfig = new JetConfig();

        jetConfig.getHazelcastConfig().addMapConfig(new MapConfig(TEST_IMAP)
                .addIndexConfig(new IndexConfig(IndexType.HASH, "indexedField")));
        initialize(1, jetConfig);

        executeSql("CREATE EXTERNAL TABLE " + TEST_IMAP + "(__key INT, indexedField VARCHAR, normalField VARCHAR) " +
                "TYPE \"" + LocalPartitionedMapConnector.TYPE_NAME + "\"");
        executeSql("CREATE EXTERNAL TABLE " + TEST_R_MAP + "(__key INT, indexedField VARCHAR, normalField VARCHAR) " +
                        "TYPE \"" + LocalReplicatedMapConnector.TYPE_NAME + "\"");
    }

    @Test
    public void select_empty() {
        assertRowsEventuallyAnyOrder("SELECT /*+ jet */ * FROM " + TEST_IMAP,
                emptyList());
    }

    @Test
    public void select_large() {
        IMap<Integer, ValueClass> map = instance().getMap(TEST_IMAP);
        Map<Integer, ValueClass> items = generateItems();
        map.putAll(items);

        assertRowsEventuallyAnyOrder("SELECT /*+ jet */ * FROM " + TEST_IMAP,
                toList(items.entrySet(), en -> new Row(en.getKey(), en.getValue().indexedField, en.getValue().normalField)));
    }

    @Test
    public void select_replicated_large() {
        ReplicatedMap<Object, Object> map = instance().getReplicatedMap(TEST_R_MAP);
        Map<Integer, ValueClass> items = generateItems();
        map.putAll(items);

        assertRowsEventuallyAnyOrder("SELECT /*+ jet */ * FROM " + TEST_R_MAP,
                toList(items.entrySet(), en -> new Row(en.getKey(), en.getValue().indexedField, en.getValue().normalField)));
    }

    @Test
    public void select_using_indexScan() {
        IMap<Integer, ValueClass> map = instance().getMap(TEST_IMAP);
        map.putAll(generateItems());

        assertRowsEventuallyAnyOrder("SELECT /*+ jet */ * FROM " + TEST_IMAP + " WHERE indexedField = 'indexed-123'",
                singletonList(new Row(123, "indexed-123", "normal-123")));
    }

    @Nonnull
    private Map<Integer, ValueClass> generateItems() {
        Map<Integer, ValueClass> items = new HashMap<>(16384);
        for (int i = 0; i < 16384; i++) {
            items.put(i, new ValueClass("indexed-" + i, "normal-" + i));
        }
        return items;
    }

    public static final class ValueClass implements Serializable {
        public String indexedField;
        public String normalField;

        public ValueClass(String indexedField, String normalField) {
            this.indexedField = indexedField;
            this.normalField = normalField;
        }

        public String getIndexedField() {
            return indexedField;
        }
    }
}
