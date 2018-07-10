/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.support.serializer;

import org.springframework.kafka.KafkaException;

/**
 * Exception returned in the consumer record value or key when a deserialization failure
 * occurs.
 *
 * @author Gary Russell
 * @since 2.2
 *
 */
@SuppressWarnings("serial")
public class DeserializationException extends KafkaException {

	private final byte[] data;

	private final boolean isKey;

	public DeserializationException(String message, byte[] data, boolean isKey, Throwable cause) {
		super(message, cause);
		this.data = data;
		this.isKey = isKey;
	}

	public byte[] getData() {
		return this.data;
	}

	public boolean isKey() {
		return this.isKey;
	}

}
