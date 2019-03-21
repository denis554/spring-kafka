/*
 * Copyright 2019 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import org.springframework.kafka.KafkaException;
import org.springframework.kafka.support.serializer.DeserializationException;

/**
 * @author Gary Russell
 * @since 2.3
 *
 */
public class SeekToCurrentErrorHandlerTests {

	@Test
	public void testClassifier() {
		AtomicReference<ConsumerRecord<?, ?>> recovered = new AtomicReference<>();
		SeekToCurrentErrorHandler handler = new SeekToCurrentErrorHandler((r, t) -> recovered.set(r));
		ConsumerRecord<String, String> record1 = new ConsumerRecord<>("foo", 0, 0L, "foo", "bar");
		ConsumerRecord<String, String> record2 = new ConsumerRecord<>("foo", 0, 1L, "foo", "bar");
		List<ConsumerRecord<?, ?>> records = Arrays.asList(record1, record2);
		IllegalStateException illegalState = new IllegalStateException();
		Consumer<?, ?> consumer = mock(Consumer.class);
		assertThatExceptionOfType(KafkaException.class).isThrownBy(() -> handler.handle(illegalState, records,
					consumer, mock(MessageListenerContainer.class)))
				.withCause(illegalState);
		handler.handle(new DeserializationException("intended", null, false, illegalState), records,
				consumer, mock(MessageListenerContainer.class));
		assertThat(recovered.get()).isSameAs(record1);
		handler.addNotRetryableException(IllegalStateException.class);
		recovered.set(null);
		handler.handle(illegalState, records, consumer, mock(MessageListenerContainer.class));
		assertThat(recovered.get()).isSameAs(record1);
		InOrder inOrder = inOrder(consumer);
		inOrder.verify(consumer).seek(new TopicPartition("foo", 0), 0L); // not recovered so seek
		inOrder.verify(consumer, times(2)).seek(new TopicPartition("foo", 0), 1L); // 2x recovered seek next
		inOrder.verifyNoMoreInteractions();
	}

}
