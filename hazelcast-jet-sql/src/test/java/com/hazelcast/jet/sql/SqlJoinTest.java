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

import com.hazelcast.jet.kafka.impl.KafkaTestSupport;
import com.hazelcast.jet.sql.impl.connector.kafka.KafkaSqlConnector;
import com.hazelcast.sql.impl.connector.LocalPartitionedMapConnector;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlJoinTest extends SqlTestSupport {

    private static final int INITIAL_PARTITION_COUNT = 4;

    private static KafkaTestSupport kafkaTestSupport;

    private String topicName;
    private String mapName;

    @BeforeClass
    public static void beforeClass() throws IOException {
        initialize(1, null);

        kafkaTestSupport = new KafkaTestSupport();
        kafkaTestSupport.createKafkaCluster();
    }

    @Before
    public void before() {
        topicName = "k_" + randomString().replace('-', '_');
        kafkaTestSupport.createTopic(topicName, INITIAL_PARTITION_COUNT);
        executeSql(format("CREATE EXTERNAL TABLE %s (__key INT, this VARCHAR) TYPE \"%s\" " +
                        "OPTIONS (" +
                        "%s '%s', " +
                        "%s '%s', " +
                        "%s '%s', " +
                        "%s '%s', " +
                        "%s '%s', " +
                        "%s '%s'" +
                        ")",
                topicName, KafkaSqlConnector.TYPE_NAME,
                "\"bootstrap.servers\"", kafkaTestSupport.getBrokerConnectionString(),
                "\"key.serializer\"", IntegerSerializer.class.getCanonicalName(),
                "\"key.deserializer\"", IntegerDeserializer.class.getCanonicalName(),
                "\"value.serializer\"", StringSerializer.class.getCanonicalName(),
                "\"value.deserializer\"", StringDeserializer.class.getCanonicalName(),
                "\"auto.offset.reset\"", "earliest")
        );

        mapName = createRandomMap();
    }

    @AfterClass
    public static void afterClass() {
        if (kafkaTestSupport != null) {
            kafkaTestSupport.shutdownKafkaCluster();
            kafkaTestSupport = null;
        }
    }

    @Test
    public void enrichment_join() {
        kafkaTestSupport.produce(topicName, 0, "kafka-value-0");
        kafkaTestSupport.produce(topicName, 1, "kafka-value-1");
        kafkaTestSupport.produce(topicName, 2, "kafka-value-2");

        instance().getMap(mapName).put(1, "map-value-1");
        instance().getMap(mapName).put(2, "map-value-2");
        instance().getMap(mapName).put(3, "map-value-3");

        assertRowsEventuallyAnyOrder(
                format("SELECT k.__key, k.this, m.__key, m.this " +
                                "FROM %s k " +
                                "JOIN %s m ON k.__key = m.__key",
                        topicName, mapName
                ),
                asList(
                        new Row(1, "kafka-value-1", 1, "map-value-1"),
                        new Row(2, "kafka-value-2", 2, "map-value-2")
                ));
    }

    @Test
    public void enrichment_join_with_filter() {
        kafkaTestSupport.produce(topicName, 0, "kafka-value-0");
        kafkaTestSupport.produce(topicName, 1, "kafka-value-1");
        kafkaTestSupport.produce(topicName, 2, "kafka-value-2");
        kafkaTestSupport.produce(topicName, 3, "kafka-value-3");

        instance().getMap(mapName).put(1, "map-value-1");
        instance().getMap(mapName).put(2, "map-value-2");
        instance().getMap(mapName).put(3, "map-value-3");

        assertRowsEventuallyAnyOrder(
                format("SELECT k.__key, m.this " +
                                "FROM %s k " +
                                "JOIN %s m ON k.__key = m.__key " +
                                "WHERE k.__key > 1 AND m.__key < 3",
                        topicName, mapName
                ),
                singletonList(
                        new Row(2, "map-value-2")
                ));
    }

    @Test
    public void enrichment_join_with_project() {
        kafkaTestSupport.produce(topicName, 1, "kafka-value-1");
        kafkaTestSupport.produce(topicName, 2, "kafka-value-2");
        kafkaTestSupport.produce(topicName, 3, "kafka-value-3");

        instance().getMap(mapName).put(0, "map-value-0");
        instance().getMap(mapName).put(1, "map-value-1");
        instance().getMap(mapName).put(2, "map-value-2");

        assertRowsEventuallyAnyOrder(
                format("SELECT k.__key + m.__key, UPPER(k.this) " +
                                "FROM %s k " +
                                "JOIN %s m ON k.__key = m.__key",
                        topicName, mapName
                ),
                asList(
                        new Row(2L, "KAFKA-VALUE-1"),
                        new Row(4L, "KAFKA-VALUE-2")
                ));
    }

    @Test
    public void enrichment_join_with_value_condition() {
        kafkaTestSupport.produce(topicName, 0, "value-0");
        kafkaTestSupport.produce(topicName, 1, "value-1");
        kafkaTestSupport.produce(topicName, 2, "value-2");

        instance().getMap(mapName).put(3, "value-1");
        instance().getMap(mapName).put(4, "value-2");
        instance().getMap(mapName).put(5, "value-3");

        assertRowsEventuallyAnyOrder(
                format("SELECT k.__key, k.this, m.__key, m.this " +
                                "FROM %s k " +
                                "JOIN %s m ON k.this = m.this",
                        topicName, mapName
                ),
                asList(
                        new Row(1, "value-1", 3, "value-1"),
                        new Row(2, "value-2", 4, "value-2")
                ));
    }

    @Test
    public void enrichment_join_with_non_equi_condition() {
        kafkaTestSupport.produce(topicName, 1, "kafka-value-1");
        kafkaTestSupport.produce(topicName, 2, "kafka-value-2");
        kafkaTestSupport.produce(topicName, 3, "kafka-value-3");

        instance().getMap(mapName).put(0, "map-value-0");
        instance().getMap(mapName).put(1, "map-value-1");
        instance().getMap(mapName).put(2, "map-value-2");

        assertRowsEventuallyAnyOrder(
                format("SELECT k.__key, m.__key " +
                                "FROM %s k " +
                                "JOIN %s m ON k.__key > m.__key",
                        topicName, mapName
                ),
                asList(
                        new Row(1, 0),
                        new Row(2, 0),
                        new Row(2, 1),
                        new Row(3, 0),
                        new Row(3, 1),
                        new Row(3, 2)
                ));
    }

    @Test
    public void enrichment_join_with_multiple_tables() {
        kafkaTestSupport.produce(topicName, 1, "kafka-value-1");
        kafkaTestSupport.produce(topicName, 2, "kafka-value-2");
        kafkaTestSupport.produce(topicName, 3, "kafka-value-3");

        instance().getMap(mapName).put(0, "map1-value-0");
        instance().getMap(mapName).put(1, "map1-value-1");
        instance().getMap(mapName).put(2, "map1-value-2");

        String anotherMapName = createRandomMap();
        instance().getMap(anotherMapName).put(0, "map2-value-0");
        instance().getMap(anotherMapName).put(1, "map2-value-1");
        instance().getMap(anotherMapName).put(2, "map2-value-2");

        assertRowsEventuallyAnyOrder(
                format("SELECT k.__key, m1.this, m2.this " +
                                "FROM %s k " +
                                "JOIN %s m1 ON k.__key = m1.__key " +
                                "JOIN %s m2 ON k.__key + m1.__key > m2.__key",
                        topicName, mapName, anotherMapName
                ),
                asList(
                        new Row(1, "map1-value-1", "map2-value-0"),
                        new Row(1, "map1-value-1", "map2-value-1"),
                        new Row(2, "map1-value-2", "map2-value-0"),
                        new Row(2, "map1-value-2", "map2-value-1"),
                        new Row(2, "map1-value-2", "map2-value-2")
                ));
    }

    @Test
    public void enrichment_join_fails_for_not_supported_connector() {
        assertThatThrownBy(() -> executeSql(
                format("SELECT 1 FROM %s k JOIN %s m ON m.__key = k.__key", mapName, topicName)
        )).hasCauseInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("Nested loop reader not supported for " + KafkaSqlConnector.class.getName());
    }

    private static String createRandomMap() {
        String mapName = "m_" + randomString().replace('-', '_');
        executeSql(
                format("CREATE EXTERNAL TABLE %s (__key INT, this VARCHAR) TYPE \"%s\"",
                        mapName, LocalPartitionedMapConnector.TYPE_NAME
                )
        );
        return mapName;
    }
}
