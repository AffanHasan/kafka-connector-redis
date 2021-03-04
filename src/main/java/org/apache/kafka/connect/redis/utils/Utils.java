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

package org.apache.kafka.connect.redis.utils;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.redis.MessageConfig;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.moilioncircle.redis.replicator.event.Event;

/**
 * Contains utility methods for Burraq service messages processing
 * 
 * @author Affan Hasan
 */
public class Utils {
	
	public static final String HEADER_REDIS_REPLICATOR_COMMAND = "redisReplicatorCommand";
	
	public static final String HEADER_CONTENT_TYPE = "contentType";
	
	public static final String MESSAGE_TYPE = "type";
	
	public static final String MESSAGE_KEY = "msgKey";
	
	public static final String HEADERS = "headers";
	
	public static final String VALUE = "value";

	public static final String FIELD = "field";

	public static final String KEY = "key";
	
	public static final String COMMAND_KEY_STRING_VALUE_HASH = "KeyStringValueHash";
	
	/**
	 * Constructor to hide explicit public constructor
	 */
	private Utils() {
	}
	
	/**
	 * Returns messages related configurations defined in connector configuration file.
	 * 
	 * @param cmd message payload
	 * @param messageConfigs list of message configuration objects
	 * @param event Redis connector CRUD event object
	 * @return {@link Map<String, Object>} Map containing configurations
	 */
	public static Map<String, Object> getMessageConfigurations(final String cmd, 
            List<MessageConfig> messageConfigs, final Event event){
        Map<String, Object> configurations = null;
        final JsonObject cmdJsonObject = parseStringToJsonObject(cmd);
        if(cmdJsonObject.has(KEY)) {
            final MessageConfig msgConfig;
            final Optional<MessageConfig> msgConfigOptional = messageConfigs.stream() //
                    .filter(msg -> Pattern.compile(msg.getType()) //
                    .matcher(decodeBase64String(cmdJsonObject.get(KEY).getAsString())) //
                    .find()) //
                    .findFirst();
            if( msgConfigOptional.isPresent() &&
                    cmdJsonObject.has( (msgConfig = msgConfigOptional.get()).getMsgKeyPath() ) && 
                    !event.getClass() //
                    .getName() //
                    .endsWith(COMMAND_KEY_STRING_VALUE_HASH)) {
                configurations = new HashMap<>();
                final ConnectHeaders headers = new ConnectHeaders();
                String redisMessageKey = event.getClass() //
                        .getName(); 
                //Set user defined configuration headers
                msgConfig.getHeaders() //
                .entrySet() //
                .forEach(entry -> headers.addString(entry.getKey(), entry.getValue()));
                // Set Redis Replicator command name as header
                headers.addString(HEADER_REDIS_REPLICATOR_COMMAND, event.getClass() //
                        .getName());
                // Set Key
                final Matcher matcher = Pattern.compile(msgConfig.getMsgKeyPattern()) //
                        .matcher(decodeBase64String(cmdJsonObject.get(msgConfig.getMsgKeyPath()).getAsString()));
                if (matcher.find()) {
                    redisMessageKey = matcher.group(1);
                }
                configurations.put(KEY, redisMessageKey);
                configurations.put(HEADERS, headers);
           }
        }
        return configurations;
    }
	
    /**
     * Loads message configuration related properties inside {@link List<MessageConfig>} passed as parameters
     * 
     * @param messageConfigs List<MessageConfig>
     * @param props properties defined in Redis connector configurations
     */
    public static void loadMessageConfigurationProperties(final List<MessageConfig> messageConfigs, final Map<String, String> props) {
        props.keySet() //
        .stream() //
        .filter(key -> Pattern.compile("msg\\.(.+)") //
                .matcher(key) //
                .find()) //
    	.forEach(messageConfig-> messageConfigs.add(new Gson().fromJson(props.get(messageConfig), MessageConfig.class)));
    }
    
    /**
     * Returns a JsonObject
     * 
     * @param string string representing a json object
     * @return parsed {@link JsonObject}
     */
    public static JsonObject parseStringToJsonObject(final String string) {
        return JsonParser.parseString(string) //
                .getAsJsonObject();
    }
    
    /**
     * Decodes a base 64 encoded string 
     * 
     * @param base64EncodedString base 64 encoded string
     * @return {@link String} decoded string
     */
    public static String decodeBase64String(final String base64EncodedString) {
        return new String(Base64.decodeBase64(base64EncodedString.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
    }
}
