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

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Objects;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.impl.connector.imap.IMapSqlConnector.TO_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.imap.IMapSqlConnector.TO_VALUE_CLASS;
import static java.lang.String.format;
import static java.time.ZoneId.systemDefault;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// TODO: move it to IMDG when INSERTs are supported, or at least move to kafka test ?
public class SqlInsertTest extends SqlTestSupport {

    private static final String INT_TO_STRING_MAP_SRC = "int_to_string_map_src";
    private static final String INT_TO_STRING_R_MAP_SRC = "int_to_string_r_map_src";
    private static final String INT_TO_STRING_MAP_SINK = "int_to_string_map_sink";

    private static final String PERSON_MAP_SINK = "person_map_sink";
    private static final String OBJECT_MAP_SINK = "object_map_sink";

    private static final String ALL_TYPES_MAP = "all_types_map";

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);

        executeSql(
                format("CREATE EXTERNAL TABLE %s (__key INT, this VARCHAR) TYPE \"%s\"",
                INT_TO_STRING_MAP_SRC, LocalPartitionedMapConnector.TYPE_NAME)
        );
        executeSql(
                format("CREATE EXTERNAL TABLE %s (__key INT, this VARCHAR) TYPE \"%s\"",
                INT_TO_STRING_R_MAP_SRC, LocalReplicatedMapConnector.TYPE_NAME)
        );
        executeSql(
                format("CREATE EXTERNAL TABLE %s (__key INT, this VARCHAR) TYPE \"%s\"",
                INT_TO_STRING_MAP_SINK, LocalPartitionedMapConnector.TYPE_NAME)
        );

        executeSql(
                format("CREATE EXTERNAL TABLE %s (id INT, birthday DATE) TYPE \"%s\" OPTIONS (%s '%s', %s '%s')",
                PERSON_MAP_SINK, LocalPartitionedMapConnector.TYPE_NAME,
                TO_KEY_CLASS, Person.class.getName(),
                TO_VALUE_CLASS, Person.class.getName())
        );

        executeSql(
                format("CREATE EXTERNAL TABLE %s (nonExistingProperty INT) TYPE \"%s\" OPTIONS (%s '%s', %s '%s')",
                OBJECT_MAP_SINK, LocalPartitionedMapConnector.TYPE_NAME,
                TO_KEY_CLASS, SerializableObject.class.getName(),
                TO_VALUE_CLASS, SerializableObject.class.getName())
        );

        // an IMap with a field of every type
        executeSql(format("CREATE EXTERNAL TABLE %s (" +
                        "__key DECIMAL(10, 0), " +
                        "string VARCHAR," +
                        "character0 CHAR, " +
                        "character1 CHARACTER, " +
                        "boolean0 BOOLEAN, " +
                        "boolean1 BOOLEAN, " +
                        "byte0 TINYINT, " +
                        "byte1 TINYINT, " +
                        "short0 SMALLINT, " +
                        "short1 SMALLINT," +
                        "int0 INT, " +
                        "int1 INTEGER," +
                        "long0 BIGINT, " +
                        "long1 BIGINT, " +
                        "bigDecimal DEC(10, 1), " +
                        "bigInteger NUMERIC(5, 0), " +
                        "float0 REAL, " +
                        "float1 FLOAT, " +
                        "double0 DOUBLE, " +
                        "double1 DOUBLE PRECISION, " +
                        "\"localTime\" TIME, " +
                        "localDate DATE, " +
                        "localDateTime TIMESTAMP, " +
                        "\"date\" TIMESTAMP WITH LOCAL TIME ZONE (\"DATE\"), " +
                        "calendar TIMESTAMP WITH TIME ZONE (\"CALENDAR\"), " +
                        "instant TIMESTAMP WITH LOCAL TIME ZONE, " +
                        "zonedDateTime TIMESTAMP WITH TIME ZONE (\"ZONED_DATE_TIME\"), " +
                        "offsetDateTime TIMESTAMP WITH TIME ZONE " +
                        /*"yearMonthInterval INTERVAL_YEAR_MONTH, " +
                        "offsetDateTime INTERVAL_DAY_SECOND, " +*/
                        ") TYPE \"%s\" OPTIONS (\"%s\" '%s')",
                ALL_TYPES_MAP, LocalPartitionedMapConnector.TYPE_NAME,
                TO_VALUE_CLASS, AllTypesValue.class.getName()
        ));
    }

    @Test
    public void insert_null() {
        assertMapEventually(
                PERSON_MAP_SINK, "INSERT OVERWRITE " + PERSON_MAP_SINK + " VALUES (null, null)",
                createMap(new Person(), new Person()));
    }

    @Test
    public void insert_toleratesNullForNonExistingProperties() {
        assertMapEventually(
                OBJECT_MAP_SINK, "INSERT OVERWRITE " + OBJECT_MAP_SINK + "(nonExistingProperty) VALUES (null)",
                createMap(new SerializableObject(), new SerializableObject()));
    }

    @Test
    public void insert_valueShadowsKey() {
        assertMapEventually(
                PERSON_MAP_SINK, "INSERT OVERWRITE " + PERSON_MAP_SINK + "(id, birthday) VALUES (1, '2020-01-01')",
                createMap(new Person(), new Person(1, LocalDate.of(2020, 1, 1))));
    }

    @Test
    public void insert_select() {
        IMap<Integer, String> intToStringMap = instance().getMap(INT_TO_STRING_MAP_SRC);
        intToStringMap.put(0, "value-0");
        intToStringMap.put(1, "value-1");

        assertMapEventually(
                INT_TO_STRING_MAP_SINK,
                "INSERT OVERWRITE " + INT_TO_STRING_MAP_SINK + " SELECT * FROM " + INT_TO_STRING_MAP_SRC,
                createMap(0, "value-0", 1, "value-1"));
    }

    @Test
    public void insert_values() {
        assertMapEventually(
                INT_TO_STRING_MAP_SINK, "INSERT OVERWRITE " + INT_TO_STRING_MAP_SINK + "(this, __key) values (2, 1)",
                createMap(1, "2"));
    }

    @Test
    public void insert_withProject() {
        assertMapEventually(
                PERSON_MAP_SINK, "INSERT OVERWRITE " + PERSON_MAP_SINK + "(birthday, id) VALUES ('2020-01-01', 0 + 1)",
                createMap(new Person(), new Person(1, LocalDate.of(2020, 1, 1))));
    }

    @Test
    public void insert_allTypes() {
        assertMapEventually(ALL_TYPES_MAP, "INSERT OVERWRITE " + ALL_TYPES_MAP + " VALUES (" +
                        "1, --key\n" +
                        "'string', --varchar\n" +
                        "'a', --character\n" +
                        "'b',\n" +
                        "true, --boolean\n" +
                        "false,\n" +
                        "126, --byte\n" +
                        "127, \n" +
                        "32766, --short\n" +
                        "32767, \n" +
                        "2147483646, --int \n" +
                        "2147483647,\n" +
                        "9223372036854775806, --long\n" +
                        "9223372036854775807,\n" +
                        // this is bigDecimal, but it's still limited to 64-bit unscaled value, see
                        // SqlValidatorImpl.validateLiteral()
                        "9223372036854775.123, --bigDecimal\n" +
                        "9223372036854775222, --bigInteger\n" +
                        "1234567890.1, --float\n" +
                        "1234567890.2, \n" +
                        "123451234567890.1, --double\n" +
                        "123451234567890.2,\n" +
                        "time'12:23:34', -- local time\n" +
                        "date'2020-04-15', -- local date \n" +
                        "timestamp'2020-04-15 12:23:34.1', --timestamp\n" +
                        // there's no timestamp-with-tz literal in calcite apparently
                        "timestamp'2020-04-15 12:23:34.2', --timestamp with tz\n" +
                        "timestamp'2020-04-15 12:23:34.3', --timestamp with tz\n" +
                        "timestamp'2020-04-15 12:23:34.4', --timestamp with tz\n" +
                        "timestamp'2020-04-15 12:23:34.5', --timestamp with tz\n" +
                        "timestamp'2020-04-15 12:23:34.6' --timestamp with tz\n" +
                        /*"INTERVAL '1' YEAR, -- year-to-month interval\n" +
                        "INTERVAL '1' HOUR -- day-to-second interval\n" +*/
                        ")",
                createMap(BigInteger.valueOf(1), new AllTypesValue(
                        "string",
                        'a',
                        'b',
                        true,
                        false,
                        (byte) 126,
                        (byte) 127,
                        (short) 32766,
                        (short) 32767,
                        2147483646,
                        2147483647,
                        9223372036854775806L,
                        9223372036854775807L,
                        new BigDecimal("9223372036854775.123"),
                        new BigInteger("9223372036854775222"),
                        1234567890.1f,
                        1234567890.2f,
                        123451234567890.1,
                        123451234567890.2,
                        LocalTime.of(12, 23, 34),
                        LocalDate.of(2020, 4, 15),
                        // TODO: should be LocalDateTime.of(2020, 4, 15, 12, 23, 34, 100_000_000)
                        //  when temporal types are fixed
                        LocalDateTime.of(2020, 4, 15, 12, 23, 34, 0),
                        Date.from(Instant.ofEpochMilli(1586953414200L)),
                        GregorianCalendar.from(ZonedDateTime.of(2020, 4, 15, 12, 23, 34, 300_000_000, UTC)
                                .withZoneSameInstant(localOffset())
                        ),
                        Instant.ofEpochMilli(1586953414400L),
                        ZonedDateTime.of(2020, 4, 15, 12, 23, 34, 500_000_000, UTC)
                                .withZoneSameInstant(localOffset()),
                        ZonedDateTime.of(2020, 4, 15, 12, 23, 34, 600_000_000, UTC)
                                .withZoneSameInstant(systemDefault())
                                .toOffsetDateTime()
                )));
    }

    private static ZoneOffset localOffset() {
        return systemDefault().getRules().getOffset(LocalDateTime.now());
    }

    @Test
    public void insert_allTypesAsStrings() {
        assertMapEventually(ALL_TYPES_MAP, "INSERT OVERWRITE " + ALL_TYPES_MAP + " VALUES (" +
                        "'1', --key\n" +
                        "'string', --varchar\n" +
                        "'a', --character\n" +
                        "'b',\n" +
                        "'true', --boolean\n" +
                        "'false',\n" +
                        "'126', --byte\n" +
                        "'127', \n" +
                        "'32766', --short\n" +
                        "'32767', \n" +
                        "'2147483646', --int \n" +
                        "'2147483647',\n" +
                        "'9223372036854775806', --long\n" +
                        "'9223372036854775807',\n" +
                        "'9223372036854775.123', --bigDecimal\n" +
                        "'9223372036854775222', --bigInteger\n" +
                        "'1234567890.1', --float\n" +
                        "'1234567890.2', \n" +
                        "'123451234567890.1', --double\n" +
                        "'123451234567890.2',\n" +
                        "'12:23:34', -- local time\n" +
                        "'2020-04-15', -- local date \n" +
                        "'2020-04-15T12:23:34.1', --timestamp\n" +
                        "'2020-04-15T12:23:34.2Z', --timestamp with tz\n" +
                        "'2020-04-15T12:23:34.3Z', --timestamp with tz\n" +
                        "'2020-04-15T12:23:34.4Z', --timestamp with tz\n" +
                        "'2020-04-15T12:23:34.5Z', --timestamp with tz\n" +
                        "'2020-04-15T12:23:34.6Z' --timestamp with tz\n" +
                        /*"INTERVAL '1' YEAR, -- year-to-month interval\n" +
                        "INTERVAL '1' HOUR -- day-to-second interval\n" +*/
                        ")",
                createMap(BigInteger.valueOf(1), new AllTypesValue(
                        "string",
                        'a',
                        'b',
                        true,
                        false,
                        (byte) 126,
                        (byte) 127,
                        (short) 32766,
                        (short) 32767,
                        2147483646,
                        2147483647,
                        9223372036854775806L,
                        9223372036854775807L,
                        new BigDecimal("9223372036854775.123"),
                        new BigInteger("9223372036854775222"),
                        1234567890.1f,
                        1234567890.2f,
                        123451234567890.1,
                        123451234567890.2,
                        LocalTime.of(12, 23, 34),
                        LocalDate.of(2020, 4, 15),
                        LocalDateTime.of(2020, 4, 15, 12, 23, 34, 100_000_000),
                        Date.from(Instant.ofEpochMilli(1586953414200L)),
                        GregorianCalendar.from(ZonedDateTime.of(2020, 4, 15, 12, 23, 34, 300_000_000, UTC)),
                        Instant.ofEpochMilli(1586953414400L),
                        ZonedDateTime.of(2020, 4, 15, 12, 23, 34, 500_000_000, UTC),
                        OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 600_000_000, UTC)
                )));
    }

    @Test
    public void insert_intoMapFails() {
        assertThatThrownBy(() -> executeSql("INSERT INTO " + PERSON_MAP_SINK + "(birthday) VALUES ('2020-01-01')"))
                .hasMessageContaining("Only INSERT OVERWRITE clause is supported for IMapSqlConnector");
    }

    @SuppressWarnings("unused")
    public static class Person implements Serializable {

        private int id;
        private LocalDate birthday;

        public Person() {
        }

        private Person(int id, LocalDate birthday) {
            this.id = id;
            this.birthday = birthday;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public LocalDate getBirthday() {
            return birthday;
        }

        public void setBirthday(LocalDate birthday) {
            this.birthday = birthday;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "id=" + id +
                    ", birthday=" + birthday +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Person person = (Person) o;
            return id == person.id &&
                    Objects.equals(birthday, person.birthday);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, birthday);
        }
    }

    public static final class SerializableObject implements Serializable {

        @Override
        public int hashCode() {
            return 1;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof SerializableObject;
        }
    }
}
