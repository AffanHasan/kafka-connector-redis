/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.connect.redis;

import static org.apache.kafka.connect.redis.utils.Utils.*;
import static java.lang.Thread.sleep;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import org.apache.kafka.connect.source.SourceRecord;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moilioncircle.redis.replicator.cmd.impl.HSetCommand;
import com.moilioncircle.redis.replicator.cmd.impl.HSetNxCommand;
import com.moilioncircle.redis.replicator.cmd.impl.LPushCommand;
import com.moilioncircle.redis.replicator.event.Event;

/**
 * Contains functional tests for {@link RedisSourceTask}
 * 
 * @author Affan Hasan
 */
public class RedisSourceTaskTest {

    public static final String PIDS_MESSAGE_PRODUCT_IDENTIFIERS = "product_identifiers";

    public static final String PIDS_MESSAGE_INDUSTRY_IDENTIFIERS = "industry_identifiers";

    public static final String PRODUCT_STATUSES = "product_statuses";

    public static final String TOPIC_PRODUCT_SPECIFICATION_STATUS_SERVICE = "product-specifications-status-service";

    public static final String TOPIC_PRODUCT_QUEUE = "product-queue";

    @Test
    public void testTaskStart() throws InterruptedException {
        final HashMap<String, String> props = new HashMap<>();
        props.put(RedisSourceConfig.USE_PSYNC2, "false");
        props.put(RedisSourceConfig.TOPIC, "fantacy");
        final RedisSourceTask task = new RedisSourceTask();
        task.start(props);
        sleep(5000);
        task.poll();
        task.stop();
    }

    @Test
    public void shouldGetSourceTaskWithJSONForEvent() throws Exception {
        final HashMap<String, String> props = new HashMap<>();
        props.put(RedisSourceConfig.USE_PSYNC2, "false");
        props.put(RedisSourceConfig.TOPIC, "test_topic");
        final RedisSourceTask task = new RedisSourceTask();
        task.start(props);
        // LPUSH key value1 value2
        final Event event = new LPushCommand("key".getBytes(StandardCharsets.UTF_8),
                new byte[][] { "value1".getBytes(StandardCharsets.UTF_8), "value2".getBytes(StandardCharsets.UTF_8) });
        final SourceRecord sourceRecord = task.getSourceRecord(event);

        assertThat(sourceRecord, is(notNullValue()));
        assertThat(sourceRecord.key(), is(notNullValue()));
        assertThat(new String((byte[])sourceRecord.key()), is(LPushCommand.class.getName()));
        assertThat(sourceRecord.value(), is(notNullValue()));
        assertThat(sourceRecord.value().toString(), is("{\"key\":\"a2V5\",\"values\":[\"dmFsdWUx\",\"dmFsdWUy\"]}"));


        final ObjectMapper mapper = new ObjectMapper();
        final LPushCommand decoded = mapper.readValue(sourceRecord.value().toString().getBytes(StandardCharsets.UTF_8),
                LPushCommand.class);
        assertThat(decoded, is(notNullValue()));
        assertThat(decoded.getKey(), is(notNullValue()));
        assertThat(new String(decoded.getKey()), is("key"));
        assertThat(decoded.getValues(), is(notNullValue()));
        assertThat(decoded.getValues().length, is(2));
        byte[][] values = decoded.getValues();
        for (int i = 0; i < values.length; i++) {
            final byte[] value = values[i];
            assertThat(new String(value), is("value" + (i + 1)));
        }
        task.stop();
    }
    
    @Test
    public void shouldSetProductIdAsMessageKeyForPIDSAddProductIdentifierCommand() throws Exception {
        final HashMap<String, String> props = new HashMap<>();
        props.put(RedisSourceConfig.USE_PSYNC2, "false");
        props.put(RedisSourceConfig.TOPIC, TOPIC_PRODUCT_QUEUE);
        props.put("msg.industry_identifier", 
                "{ \"type\" : \"industry_identifiers:(.+)\", \"msgKeyPath\" : \"value\", \"msgKeyPattern\" : \"(.+)\" , \"headers\" : { \"contentType\" : \"application/PIDS-industry_identifier\" } }");
        props.put("msg.product_identifier", 
                "{ \"type\" : \"product_identifiers\", \"msgKeyPath\" : \"field\", \"msgKeyPattern\" : \"(.+)\" , \"headers\" : { \"contentType\" : \"application/PIDS-product_identifier\" } }");
        props.put("msg.industry_definition", 
                "{ \"type\" : \"definitions:(.+)\", \"msgKeyPath\" :\"key\", \"msgKeyPattern\" : \"definitions:(.+)\", \"headers\" : { \"contentType\" : \"application/PIDS-industry_definition\" } }	");
        final RedisSourceTask task = new RedisSourceTask();
        task.start(props);
        final String productId = "ce10fe56-85b7-4613-aec8-c386c1dc0a97";
        // HSetNx Command for add product identifier
        final Event event = new HSetNxCommand(PIDS_MESSAGE_PRODUCT_IDENTIFIERS.getBytes(StandardCharsets.UTF_8), 
                productId.getBytes(StandardCharsets.UTF_8), 
                "Author:Pappu|ISBN:11-2232320-32232SN".getBytes(StandardCharsets.UTF_8));

        final SourceRecord sourceRecord = task.getSourceRecord(event);
        // Verify Custom Configuration Headers
        assertThat(sourceRecord.headers() //
                .allWithName(HEADER_CONTENT_TYPE) //
                .next() //
                .value(), is("application/PIDS-product_identifier"));
        assertThat(sourceRecord.headers() //
                .allWithName(HEADER_REDIS_REPLICATOR_COMMAND) //
                .next() //
                .value(), is("com.moilioncircle.redis.replicator.cmd.impl.HSetNxCommand"));
        // BASE 64 Verifications
        assertThat(sourceRecord, is(notNullValue()));
        assertThat(sourceRecord.key(), is(notNullValue()));
        assertThat(new String((byte[])sourceRecord.key()), is(productId));
        assertThat(sourceRecord.value(), is(notNullValue()));
        assertThat(sourceRecord.value().toString(), is("{\"key\":\"cHJvZHVjdF9pZGVudGlmaWVycw==\",\"field\":\"Y2UxMGZlNTYtODViNy00NjEzLWFlYzgtYzM4NmMxZGMwYTk3\","
                + "\"value\":\"QXV0aG9yOlBhcHB1fElTQk46MTEtMjIzMjMyMC0zMjIzMlNO\"}"));

        // Decoded Verifications
        final ObjectMapper mapper = new ObjectMapper();
        final HSetNxCommand decoded = mapper.readValue(sourceRecord.value().toString().getBytes(StandardCharsets.UTF_8),
                HSetNxCommand.class);
        assertThat(decoded, is(notNullValue()));
        assertThat(decoded.getKey(), is(notNullValue()));
        assertThat(new String(decoded.getKey()), is(PIDS_MESSAGE_PRODUCT_IDENTIFIERS));
        assertThat(decoded.getField(), is(notNullValue()));
        assertThat(new String(decoded.getField()), is(productId));
        assertThat(decoded.getValue(), is(notNullValue()));
        assertThat(new String(decoded.getValue()), is("Author:Pappu|ISBN:11-2232320-32232SN"));
        task.stop();
    }

    @Test
    public void shouldSetProductIdAsMessageKeyForPIDSAddIndustryIdentifierCommand() throws Exception {
        final HashMap<String, String> props = new HashMap<>();
        props.put(RedisSourceConfig.USE_PSYNC2, "false");
        props.put(RedisSourceConfig.TOPIC, TOPIC_PRODUCT_QUEUE);
        props.put("msg.industry_identifier", 
                "{ \"type\" : \"industry_identifiers:(.+)\", \"msgKeyPath\" : \"value\", \"msgKeyPattern\" : \"(.+)\" , \"headers\" : { \"contentType\" : \"application/PIDS-industry_identifier\" } }");
        props.put("msg.product_identifier", 
                "{ \"type\" : \"product_identifiers\", \"msgKeyPath\" : \"field\", \"msgKeyPattern\" : \"(.+)\" , \"headers\" : { \"contentType\" : \"application/PIDS-product_identifier\" } }");
        props.put("msg.industry_definition", 
                "{ \"type\" : \"definitions:(.+)\", \"msgKeyPath\" :\"key\", \"msgKeyPattern\" : \"definitions:(.+)\", \"headers\" : { \"contentType\" : \"application/PIDS-industry_definition\" } }	");
        final RedisSourceTask task = new RedisSourceTask();
        task.start(props);
        final String productId = "ce10fe56-85b7-4613-aec8-c386c1dc0a97";
        // HSetNx Command for add product identifier
        final Event event = new HSetNxCommand("industry_identifiers:Book".getBytes(StandardCharsets.UTF_8), 
                "Author:Pappu|ISBN:11-2232320-32232SN".getBytes(StandardCharsets.UTF_8), 
                productId.getBytes(StandardCharsets.UTF_8));

        final SourceRecord sourceRecord = task.getSourceRecord(event);
        // Verify Custom Configuration Headers
        assertThat(sourceRecord.headers() //
                .allWithName(HEADER_CONTENT_TYPE) //
                .next() //
                .value(), is("application/PIDS-industry_identifier"));
        assertThat(sourceRecord.headers() //
                .allWithName(HEADER_REDIS_REPLICATOR_COMMAND) //
                .next() //
                .value(), is("com.moilioncircle.redis.replicator.cmd.impl.HSetNxCommand"));
        // BASE 64 Verifications
        assertThat(sourceRecord, is(notNullValue()));
        assertThat(sourceRecord.key(), is(notNullValue()));
        assertThat(new String((byte[])sourceRecord.key()), is(productId));
        assertThat(sourceRecord.value(), is(notNullValue()));
        assertThat(sourceRecord.value().toString(), is("{\"key\":\"aW5kdXN0cnlfaWRlbnRpZmllcnM6Qm9vaw==\",\"field\":\"QXV0aG9yOlBhcHB1fElTQk46MTEtMjIzMjMyMC0zMjIzMlNO\","
        		+ "\"value\":\"Y2UxMGZlNTYtODViNy00NjEzLWFlYzgtYzM4NmMxZGMwYTk3\"}"));

        // Decoded Verifications
        final ObjectMapper mapper = new ObjectMapper();
        final HSetNxCommand decoded = mapper.readValue(sourceRecord.value().toString().getBytes(StandardCharsets.UTF_8),
        		HSetNxCommand.class);
        assertThat(decoded, is(notNullValue()));
        assertThat(decoded.getKey(), is(notNullValue()));
        assertThat(new String(decoded.getKey()), startsWith(PIDS_MESSAGE_INDUSTRY_IDENTIFIERS));
        assertThat(decoded.getField(), is(notNullValue()));
        assertThat(new String(decoded.getField()), is("Author:Pappu|ISBN:11-2232320-32232SN"));
        assertThat(decoded.getValue(), is(notNullValue()));
        assertThat(new String(decoded.getValue()), is(productId));
        task.stop();
    }

    @Test
    public void shouldSetProductIdAsMessageKeyForPSSSCreateProductSpecsStatusCommand() throws Exception {
        final HashMap<String, String> props = new HashMap<>();
        props.put(RedisSourceConfig.USE_PSYNC2, "false");
        props.put(RedisSourceConfig.TOPIC, TOPIC_PRODUCT_SPECIFICATION_STATUS_SERVICE);
        props.put("msg.product_statuses", 
                "{ \"type\" : \"product_statuses:(.+)\", \"msgKeyPath\" : \"key\", \"msgKeyPattern\" : \"product_statuses:(.+)\" , \"headers\" : { \"contentType\" : \"application/PSSS-product_statuses\" } }");
        final RedisSourceTask task = new RedisSourceTask();
        task.start(props);
        final String productId = "ce10fe56-85b7-4613-aec8-c386c1dc0a97";
        // HSetCommand for create new product specification status
        final Event event = new HSetCommand(("product_statuses:" + productId).getBytes(StandardCharsets.UTF_8),
                "en_CA".getBytes(StandardCharsets.UTF_8),
                "COMPLETED".getBytes(StandardCharsets.UTF_8));

        final SourceRecord sourceRecord = task.getSourceRecord(event);
        // Verify Custom Configuration Headers
        assertThat(sourceRecord.headers() //
                .allWithName(HEADER_CONTENT_TYPE) //
                .next() //
                .value(), is("application/PSSS-product_statuses"));
        assertThat(sourceRecord.headers() //
                .allWithName(HEADER_REDIS_REPLICATOR_COMMAND) //
                .next() //
                .value(), is("com.moilioncircle.redis.replicator.cmd.impl.HSetCommand"));
        // BASE 64 Verifications
        assertThat(sourceRecord, is(notNullValue()));
        assertThat(sourceRecord.key(), is(notNullValue()));
        assertThat(new String((byte[])sourceRecord.key()), is(productId));
        assertThat(sourceRecord.value(), is(notNullValue()));
        assertThat(sourceRecord.value().toString(),
                is("{\"key\":\"cHJvZHVjdF9zdGF0dXNlczpjZTEwZmU1Ni04NWI3LTQ2MTMtYWVjOC1jMzg2YzFkYzBhOTc=\",\"field\":\"ZW5fQ0E=\",\"value\":\"Q09NUExFVEVE\"}"));

        // Decoded Verifications
        final ObjectMapper mapper = new ObjectMapper();
        final HSetNxCommand decoded = mapper.readValue(sourceRecord.value().toString().getBytes(StandardCharsets.UTF_8),
                HSetNxCommand.class);
        assertThat(decoded, is(notNullValue()));
        assertThat(decoded.getKey(), is(notNullValue()));
        assertThat(new String(decoded.getKey()), startsWith(PRODUCT_STATUSES));
        assertThat(decoded.getField(), is(notNullValue()));
        assertThat(new String(decoded.getField()), is("en_CA"));
        assertThat(decoded.getValue(), is(notNullValue()));
        assertThat(new String(decoded.getValue()), is("COMPLETED"));
        task.stop();
    }

    @Test
    public void shouldSetProductIdAsMessageKeyForPIDSEVALBasedOperation() throws Exception {
        final HashMap<String, String> props = new HashMap<>();
        props.put(RedisSourceConfig.USE_PSYNC2, "false");
        props.put(RedisSourceConfig.TOPIC, TOPIC_PRODUCT_QUEUE);
        props.put("msg.industry_identifier", 
                "{ \"type\" : \"industry_identifiers:(.+)\", \"msgKeyPath\" : \"value\", \"msgKeyPattern\" : \"(.+)\" , \"headers\" : { \"contentType\" : \"application/PIDS-industry_identifier\" } }");
        props.put("msg.product_identifier", 
                "{ \"type\" : \"product_identifiers\", \"msgKeyPath\" : \"field\", \"msgKeyPattern\" : \"(.+)\" , \"headers\" : { \"contentType\" : \"application/PIDS-product_identifier\" } }");
        props.put("msg.industry_definition", 
                "{ \"type\" : \"definitions:(.+)\", \"msgKeyPath\" :\"key\", \"msgKeyPattern\" : \"definitions:(.+)\", \"headers\" : { \"contentType\" : \"application/PIDS-industry_definition\" } }	");
        final RedisSourceTask task = new RedisSourceTask();
        task.start(props);
        final String productId = "1d9fb2e2-ac3e-4d75-b39d-bf4dbfe29a36";
        // HSetNx Command for add product identifier
        final Event eventIndustryIdentifier = new HSetNxCommand("industry_identifiers:foodindustry".getBytes(StandardCharsets.UTF_8), 
                "dinner:butter02|snacks:fries01".getBytes(StandardCharsets.UTF_8), 
                productId.getBytes(StandardCharsets.UTF_8));
        final Event eventProductIdentifier = new HSetNxCommand("product_identifiers".getBytes(StandardCharsets.UTF_8), 
                productId.getBytes(StandardCharsets.UTF_8), 
                "foodindustry<>dinner:butter02|snacks:fries01".getBytes(StandardCharsets.UTF_8));

        final SourceRecord sourceRecordIndustryIdentifier = task.getSourceRecord(eventIndustryIdentifier);
        final SourceRecord sourceRecordProductIdentifier = task.getSourceRecord(eventProductIdentifier);
        // Verify Custom Configuration Headers
        assertThat(sourceRecordIndustryIdentifier.headers() //
                .allWithName(HEADER_CONTENT_TYPE) //
                .next() //
                .value(), is("application/PIDS-industry_identifier"));
        assertThat(sourceRecordProductIdentifier.headers() //
                .allWithName(HEADER_CONTENT_TYPE) //
                .next() //
                .value(), is("application/PIDS-product_identifier"));
        assertThat(sourceRecordIndustryIdentifier.headers() //
                .allWithName(HEADER_REDIS_REPLICATOR_COMMAND) //
                .next() //
                .value(), is("com.moilioncircle.redis.replicator.cmd.impl.HSetNxCommand"));
        assertThat(sourceRecordProductIdentifier.headers() //
                .allWithName(HEADER_REDIS_REPLICATOR_COMMAND) //
                .next() //
                .value(), is("com.moilioncircle.redis.replicator.cmd.impl.HSetNxCommand"));
        // BASE 64 Verifications
        assertThat(sourceRecordIndustryIdentifier, is(notNullValue()));
        assertThat(sourceRecordProductIdentifier, is(notNullValue()));
        assertThat(sourceRecordIndustryIdentifier.key(), is(notNullValue()));
        assertThat(sourceRecordProductIdentifier.key(), is(notNullValue()));
        assertThat(new String((byte[])sourceRecordIndustryIdentifier.key()), is(productId));
        assertThat(new String((byte[])sourceRecordProductIdentifier.key()), is(productId));
        assertThat(sourceRecordIndustryIdentifier.value(), is(notNullValue()));
        assertThat(sourceRecordProductIdentifier.value(), is(notNullValue()));
        assertThat(sourceRecordIndustryIdentifier.value().toString(), is("{\"key\":\"aW5kdXN0cnlfaWRlbnRpZmllcnM6Zm9vZGluZHVzdHJ5\",\"field\":\"ZGlubmVyOmJ1dHRlcjAyfHNuYWNrczpmcmllczAx\",\"value\":\"MWQ5ZmIyZTItYWMzZS00ZDc1LWIzOWQtYmY0ZGJmZTI5YTM2\"}"));
        assertThat(sourceRecordProductIdentifier.value().toString(), is("{\"key\":\"cHJvZHVjdF9pZGVudGlmaWVycw==\",\"field\":\"MWQ5ZmIyZTItYWMzZS00ZDc1LWIzOWQtYmY0ZGJmZTI5YTM2\",\"value\":\"Zm9vZGluZHVzdHJ5PD5kaW5uZXI6YnV0dGVyMDJ8c25hY2tzOmZyaWVzMDE=\"}"));

        // Decoded Verifications
        final ObjectMapper mapper = new ObjectMapper();
        final HSetNxCommand decodedIndustryIdentifier = mapper.readValue(sourceRecordIndustryIdentifier.value().toString().getBytes(StandardCharsets.UTF_8),
                HSetNxCommand.class);
        final HSetNxCommand decodedProductIdentifier = mapper.readValue(sourceRecordProductIdentifier.value().toString().getBytes(StandardCharsets.UTF_8),
                HSetNxCommand.class);
        assertThat(decodedIndustryIdentifier, is(notNullValue()));
        assertThat(decodedProductIdentifier, is(notNullValue()));
        assertThat(decodedIndustryIdentifier.getKey(), is(notNullValue()));
        assertThat(decodedProductIdentifier.getKey(), is(notNullValue()));
        assertThat(new String(decodedIndustryIdentifier.getKey()), startsWith(PIDS_MESSAGE_INDUSTRY_IDENTIFIERS));
        assertThat(new String(decodedProductIdentifier.getKey()), startsWith(PIDS_MESSAGE_PRODUCT_IDENTIFIERS));
        assertThat(decodedIndustryIdentifier.getField(), is(notNullValue()));
        assertThat(decodedProductIdentifier.getField(), is(notNullValue()));
        assertThat(new String(decodedIndustryIdentifier.getField()), is("dinner:butter02|snacks:fries01"));
        assertThat(new String(decodedProductIdentifier.getField()), is(productId));
        assertThat(decodedIndustryIdentifier.getValue(), is(notNullValue()));
        assertThat(decodedProductIdentifier.getValue(), is(notNullValue()));
        assertThat(new String(decodedIndustryIdentifier.getValue()), is(productId));
        assertThat(new String(decodedProductIdentifier.getValue()), is("foodindustry<>dinner:butter02|snacks:fries01"));
        task.stop();
    }
}
