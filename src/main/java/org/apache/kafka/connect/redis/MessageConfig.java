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

import java.util.Map;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Contains individual message configurations for Kafka Connect Redis messages 
 * 
 * @author Affan Hasan
 */
public class MessageConfig {

	private final String type;
	
	private final String msgKeyPath;
	
	private final String msgKeyPattern;
	
	private final Map<String, String> headers;
	
	/**
	 * Default constructor
	 *
	 * @param type message type
	 * @param msgKeyPath property
	 * @param msgKeyPattern regex pattern
	 * @param headers custom headers
	 */
	public MessageConfig(final String type, final String msgKeyPath,
	        final String msgKeyPattern,
	        final Map<String, String> headers) {
        this.type = type;
        this.msgKeyPath = msgKeyPath;
        this.msgKeyPattern = msgKeyPattern;
        this.headers = headers;
	}

	/**
	 * Return message type
	 *
	 * @return the type
	 */
	public final String getType() {
        return type;
	}

	/**
	 * Return property name
	 *
	 * @return the msgKeyPath
	 */
	public final String getMsgKeyPath() {
        return msgKeyPath;
	}

	/**
	 * Return regex pattern
	 *
	 * @return the msgKeyPattern
	 */
	public final String getMsgKeyPattern() {
        return msgKeyPattern;
	}

	/**
	 * Return custom headers
	 *
	 * @return the headers
	 */
	public final Map<String, String> getHeaders() {
        return headers;
	}

    @Override
    public final int hashCode() {
        return new HashCodeBuilder() //
                .append(type) //
                .append(msgKeyPath) //
                .append(msgKeyPattern) //
                .toHashCode();
    }

    @Override
    public final boolean equals(final Object object) {
        if (object == null) {
            return false;
        }
        if (object == this) {
            return true;
        }
        if (!(object instanceof MessageConfig)) {
            return false;
        }
        final MessageConfig message = (MessageConfig) object;
        return new EqualsBuilder() //
                .append(type, message.getType()) //
                .append(msgKeyPath, message.getMsgKeyPath()) //
                .append(msgKeyPattern, message.getMsgKeyPattern()) //
                .isEquals();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this) //
                .append("Type", getType()) //
                .append("MsgKeyPath", getMsgKeyPath()) //
                .append("MsgKeyPattern", getMsgKeyPattern()) //
                .toString();
    }
}
