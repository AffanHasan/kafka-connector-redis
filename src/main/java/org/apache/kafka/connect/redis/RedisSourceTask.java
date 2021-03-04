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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moilioncircle.redis.replicator.event.Event;

/**
 * Generic Source task specific to BURRAQ Redis based services.
 * 
 * @author Affan Hasan
 */
public class RedisSourceTask extends SourceTask {
	
    private static final Logger log = LoggerFactory.getLogger(RedisSourceTask.class);

    private long in_memory_event_size;
    private double memory_ratio;
    private String event_cache_file_name;
    private RedisBacklogEventBuffer eventBuffer;
    private final ObjectMapper mapper = new ObjectMapper();
    private String topic;
    private List<MessageConfig> messageConfigs = new ArrayList<>();

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    /**
     * This method is called upon redis connector start-up
     */
    @Override
    public void start(final Map<String, String> props) {
        // Load user-defined message configurations
        loadMessageConfigurationProperties(messageConfigs, props);
        final Map<String, Object> configuration = RedisSourceConfig.CONFIG_DEF.parse(props);
        in_memory_event_size = (long) configuration.get(RedisSourceConfig.IN_MEMORY_EVENT_SIZE);
        memory_ratio = (double) configuration.get(RedisSourceConfig.MEMORY_RATIO);
        event_cache_file_name = (String) configuration.get(RedisSourceConfig.EVENT_CACHE_FILE);
        topic = (String) configuration.get(RedisSourceConfig.TOPIC);
        eventBuffer = new RedisBacklogEventBuffer(in_memory_event_size, memory_ratio, event_cache_file_name);

        final RedisPartialSyncWorker psyncWorker = new RedisPartialSyncWorker(eventBuffer, props);
        final Thread workerThread = new Thread(psyncWorker);
        workerThread.start();
    }

    /**
     * Connector runtime calls this method in order to fetch database records to send
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        final ArrayList<SourceRecord> records = new ArrayList<>();
        final Event event = eventBuffer.poll();
        if (event != null) {
            final SourceRecord sourceRecord = getSourceRecord(event);
            if (sourceRecord != null) {
                log.debug("Source Record: {}", sourceRecord);
                log.info("Publishing record for command: {} ", event.getClass().getName());
                log.info("The record payload is: {}", sourceRecord);
                records.add(sourceRecord);
            }
        }
        return records;
    }

    /**
     * Returns source records
     * 
     * @param event Redis CRUD event
     * @return {@link SourceRecord} Kafka connect {@link SourceRecord}
     */
    public SourceRecord getSourceRecord(final Event event) {
        SourceRecord record = null;
        final Map<String, String> partition = Collections.singletonMap(RedisSourceConfig.SOURCE_PARTITION_KEY, RedisSourceConfig.SOURCE_PARTITION_VALUE);
        final SchemaBuilder bytesSchema = SchemaBuilder.bytes();

        // Redis backlog has no offset or time stamp
        final Timestamp ts = new Timestamp(System.currentTimeMillis()); // avoid invalid time stamp exception
        final long timestamp = ts.getTime();
        // Set time stamp as offset
        final Map<String, ?> offset = Collections.singletonMap(RedisSourceConfig.OFFSET_KEY, timestamp);
        try {
            final String cmd = mapper.writeValueAsString(event);
            final Map<String, Object> configurations = getMessageConfigurations(cmd, messageConfigs, event);
            if(null == configurations) { // If no configuration has been defined for this message
                record = new SourceRecord(partition, offset, this.topic, null, bytesSchema, event.getClass() //
                        .getName() //
                        .getBytes() , null, cmd, timestamp);
            } else { // If a configuration has been defined for this message
                record = new SourceRecord(partition, offset, this.topic, null, bytesSchema, ((String)configurations.get(KEY)).getBytes()
                        , null, cmd, timestamp, ((ConnectHeaders)configurations.get(HEADERS)));
            }
        } catch (final JsonProcessingException e) {
            log.error("Error converting event to JSON", e);
        }
        return record;
    }

    /**
     * This method is called upon connector stop
     */
    @Override
    public void stop() {
    }
}
