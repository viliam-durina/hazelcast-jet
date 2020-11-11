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

import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.fail;

public class SqlWindowAggregateTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        // TODO change to 2
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_noGrouping() {
        sqlService.execute(javaSerializableMapDdl("m", String.class, OffsetDateTime.class));
        sqlService.execute("SELECT * FROM TABLE(TUMBLE(TABLE m, DESCRIPTOR(this), interval '5' seconds))");
    }

    @Test
    public void test_groupingByBoundaryOnly() {
        sqlService.execute(javaSerializableMapDdl("m", String.class, OffsetDateTime.class));
        sqlService.execute("SELECT window_start, count(__key) " +
                "FROM TABLE(TUMBLE(TABLE m, DESCRIPTOR(this), interval '5' seconds)) " +
                "GROUP BY window_start");
    }

    @Test
    public void test_groupingByBoundaryAndField() {
        sqlService.execute(javaSerializableMapDdl("m", String.class, OffsetDateTime.class));
        sqlService.execute("SELECT window_start, this, count(__key) " +
                "FROM TABLE(TUMBLE(TABLE m, DESCRIPTOR(this), interval '5' seconds)) " +
                "GROUP BY window_start, this");
    }

    @Test
    public void test_groupingFieldOnly() {
        sqlService.execute(javaSerializableMapDdl("m", String.class, OffsetDateTime.class));
        sqlService.execute("SELECT this, count(__key) " +
                "FROM TABLE(TUMBLE(TABLE m, DESCRIPTOR(this), interval '5' seconds)) " +
                "GROUP BY this");
    }

    @Test
    public void test_noWindowBoundarySelected() {
        sqlService.execute(javaSerializableMapDdl("m", String.class, OffsetDateTime.class));
        sqlService.execute("SELECT count(__key) " +
                "FROM TABLE(TUMBLE(TABLE m, DESCRIPTOR(this), interval '5' seconds)) " +
                "GROUP BY window_start");
    }

    @Test
    public void when_monthInterval_then_fail() {
        sqlService.execute(javaSerializableMapDdl("m", String.class, OffsetDateTime.class));
        assertThatThrownBy(() -> sqlService.execute("SELECT * " +
                "FROM TABLE(TUMBLE(TABLE m, DESCRIPTOR(this), INTERVAL '5' MONTHS))"))
                .hasMessage("foo");
    }

    @Test
    public void when_twoColumnsInDescriptor_then_fail() {
        sqlService.execute(javaSerializableMapDdl("m", String.class, OffsetDateTime.class));
        assertThatThrownBy(() -> sqlService.execute("SELECT * " +
                "FROM TABLE(TUMBLE(TABLE m, DESCRIPTOR(this, __key), INTERVAL '5' SECONDS))"))
                .hasMessage("foo");
    }

    @Test
    public void when_wrongTypeInTimeColumn_then_fail() {
        sqlService.execute(javaSerializableMapDdl("m", String.class, OffsetDateTime.class));
        assertThatThrownBy(() -> sqlService.execute("SELECT * " +
                "FROM TABLE(TUMBLE(TABLE m, DESCRIPTOR(__key), INTERVAL '5' SECONDS))"))
                .hasMessage("foo");
    }

    @Test
    public void when_sizeNotAnIntegerMultipleOfSize_then_fail() {
        sqlService.execute(javaSerializableMapDdl("m", String.class, OffsetDateTime.class));
        assertThatThrownBy(() -> sqlService.execute("SELECT * " +
                "FROM TABLE(HOP(TABLE m, DESCRIPTOR(__key), INTERVAL '3' SECONDS, INTERVAL '5' SECONDS))"))
                .hasMessage("foo");
    }

    @Test
    public void when_intervalsOutOfRange_then_fail() {
        sqlService.execute(javaSerializableMapDdl("m", String.class, OffsetDateTime.class));

        testIntervals("TUMBLE", "INTERVAL '-1' SECONDS");
        testIntervals("TUMBLE", "INTERVAL '0' SECONDS");
        testIntervals("HOP", "INTERVAL '-1' SECONDS, INTERVAL '5' SECONDS");
        testIntervals("HOP", "INTERVAL '5' SECONDS, INTERVAL '-1' SECONDS");
        testIntervals("HOP", "INTERVAL '0' SECONDS, INTERVAL '5' SECONDS");
        testIntervals("HOP", "INTERVAL '5' SECONDS, INTERVAL '0' SECONDS");
        testIntervals("HOP", "INTERVAL '-2' SECONDS, INTERVAL '-1' SECONDS");
        testIntervals("HOP", "INTERVAL '0' SECONDS, INTERVAL '0' SECONDS");
    }

    private void testIntervals(String function, String intervals) {
        // no grouping
        assertThatThrownBy(() -> sqlService.execute("SELECT * " +
                "FROM TABLE(" + function + "(TABLE m, DESCRIPTOR(__key), " + intervals + "))"))
                .hasMessage("foo");

        // with grouping
        assertThatThrownBy(() -> sqlService.execute("SELECT window_start, COUNT(*) " +
                "FROM TABLE(" + function + "(TABLE m, DESCRIPTOR(__key), " + intervals + ")) " +
                "GROUP BY window_start"))
                .hasMessage("foo");
    }

    @Test
    public void when_notGroupingByWindowStart_then_fail() {
        fail("todo");
    }

    @Test
    public void test_groupByOneWindowEnd_and_aggregateTheOther() {
        sqlService.execute(javaSerializableMapDdl("m", String.class, OffsetDateTime.class));
        sqlService.execute("SELECT window_start, COUNT(window_end), SUM(this) " +
                "FROM TABLE(TUMBLE(TABLE m, DESCRIPTOR(__key), INTERVAL '1' SECOND) " +
                "GROUP BY window_start");

        sqlService.execute("SELECT window_end, COUNT(window_start), SUM(this) " +
                "FROM TABLE(TUMBLE(TABLE m, DESCRIPTOR(__key), INTERVAL '1' SECOND) " +
                "GROUP BY window_end");
    }

    @Test
    public void test_multipleWindowBoundariesInGroup() {
        sqlService.execute(javaSerializableMapDdl("m", String.class, OffsetDateTime.class));
        sqlService.execute("SELECT window_start, COUNT(window_end), SUM(this) " +
                "FROM TABLE(TUMBLE(TABLE m, DESCRIPTOR(__key), INTERVAL '1' SECOND) " +
                "GROUP BY window_start, window_end, window_start, window_end");
    }

    @Test
    public void when_groupExpressionWithWindowBoundary_then_fail() {
        sqlService.execute(javaSerializableMapDdl("m", String.class, OffsetDateTime.class));
        // this could in theory work because we add a constant, but we don't support it
        sqlService.execute("SELECT window_start, COUNT(window_end), SUM(this) " +
                "FROM TABLE(TUMBLE(TABLE m, DESCRIPTOR(__key), INTERVAL '1' SECOND)) " +
                "GROUP BY window_start + INTERVAL '1' SECOND");
    }

    @Test
    public void test_windowAggrWithoutProject() {
        // The rule matches "Aggr<-Project<-TableFunctionScan". However, in some cases,
        // the Project might be missing. This test tries to produce such a case and checks
        // that it works
        sqlService.execute(javaSerializableMapDdl("m", String.class, OffsetDateTime.class));
        // this could in theory work because we add a constant, but we don't support it
        sqlService.execute("SELECT MAX(__key), COUNT(this), window_start, window_end " +
                "FROM TABLE(TUMBLE(TABLE m, DESCRIPTOR(__key), INTERVAL '1' SECOND)) " +
                "GROUP BY window_start, window_end");
    }
}
