/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.listener;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer2;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Listener utilities.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
public final class ListenerUtils {

	private ListenerUtils() {
		super();
	}

	public static ListenerType determineListenerType(Object listener) {
		Assert.notNull(listener, "Listener cannot be null");
		ListenerType listenerType;
		if (listener instanceof AcknowledgingConsumerAwareMessageListener
				|| listener instanceof BatchAcknowledgingConsumerAwareMessageListener) {
			listenerType = ListenerType.ACKNOWLEDGING_CONSUMER_AWARE;
		}
		else if (listener instanceof ConsumerAwareMessageListener
				|| listener instanceof BatchConsumerAwareMessageListener) {
			listenerType = ListenerType.CONSUMER_AWARE;
		}
		else if (listener instanceof AcknowledgingMessageListener
				|| listener instanceof BatchAcknowledgingMessageListener) {
			listenerType = ListenerType.ACKNOWLEDGING;
		}
		else if (listener instanceof GenericMessageListener) {
			listenerType = ListenerType.SIMPLE;
		}
		else {
			throw new IllegalArgumentException("Unsupported listener type: " + listener.getClass().getName());
		}
		return listenerType;
	}

	/**
	 * Extract a {@link DeserializationException} from the supplied header name, if
	 * present.
	 * @param record the consumer record.
	 * @param headerName the header name.
	 * @param logger the logger for logging errors.
	 * @return the exception or null.
	 * @since 2.3
	 */
	@Nullable
	public static DeserializationException getExceptionFromHeader(final ConsumerRecord<?, ?> record,
			String headerName, Log logger) {

		Header header = record.headers().lastHeader(headerName);
		if (header != null) {
			try {
				DeserializationException ex = (DeserializationException) new ObjectInputStream(
						new ByteArrayInputStream(header.value())).readObject();
				Headers headers = new RecordHeaders(Arrays.stream(record.headers().toArray())
						.filter(h -> !h.key()
								.startsWith(ErrorHandlingDeserializer2.KEY_DESERIALIZER_EXCEPTION_HEADER_PREFIX))
						.collect(Collectors.toList()));
				ex.setHeaders(headers);
				return ex;
			}
			catch (IOException | ClassNotFoundException | ClassCastException e) {
				logger.error("Failed to deserialize a deserialization exception", e);
			}
		}
		return null;
	}

}
