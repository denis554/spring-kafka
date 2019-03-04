/*
 * Copyright 2018-2019 the original author or authors.
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

package org.springframework.kafka.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer2;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * @author Gary Russell
 * @since 2.2
 *
 */
public class SeekToCurrentRecovererTests {

	private static String topic1 = "seekTopic1";

	private static String topic1DLT = "seekTopic1.FOO";

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true, topic1, topic1DLT);

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

	@Test
	public void testMaxFailures() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps("seekTestMaxFailures", "false", embeddedKafka);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props, null,
				new ErrorHandlingDeserializer2<>(new JsonDeserializer<>(String.class)));
		ContainerProperties containerProps = new ContainerProperties(topic1);
		containerProps.setPollTimeout(10_000);

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		senderProps.put(ProducerConfig.RETRIES_CONFIG, 1);
		DefaultKafkaProducerFactory<Object, Object> pf = new DefaultKafkaProducerFactory<>(senderProps);
		final KafkaTemplate<Object, Object> template = new KafkaTemplate<>(pf);
		Serializer<?> byteArraySerializer = new ByteArraySerializer();
		@SuppressWarnings("unchecked")
		DefaultKafkaProducerFactory<Object, Object> dltPf =
				new DefaultKafkaProducerFactory<Object, Object>(senderProps, null, (Serializer<Object>) byteArraySerializer);
		final CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<String> data = new AtomicReference<>();
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			data.set(message.value());
			if (message.offset() == 0) {
				throw new ListenerExecutionFailedException("fail for max failures");
			}
			latch.countDown();
		});

		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("testSeekMaxFailures");
		final CountDownLatch recoverLatch = new CountDownLatch(1);
		final AtomicReference<String> failedGroupId = new AtomicReference<>();
		Map<Class<?>, KafkaTemplate<?, ?>> templates = new LinkedHashMap<>();
		templates.put(String.class, template);
		templates.put(byte[].class, new KafkaTemplate<>(dltPf));
		DeadLetterPublishingRecoverer recoverer =
				new DeadLetterPublishingRecoverer(templates,
						(r, e) -> new TopicPartition(topic1DLT, r.partition())) {

			@Override
			public void accept(ConsumerRecord<?, ?> record, Exception exception) {
				super.accept(record, exception);
				if (exception instanceof ListenerExecutionFailedException) {
					failedGroupId.set(((ListenerExecutionFailedException) exception).getGroupId());
				}
				recoverLatch.countDown();
			}

		};
		SeekToCurrentErrorHandler errorHandler = spy(new SeekToCurrentErrorHandler(recoverer, 3));
		container.setErrorHandler(errorHandler);
		final CountDownLatch stopLatch = new CountDownLatch(1);
		container.setApplicationEventPublisher(e -> {
			if (e instanceof ConsumerStoppedEvent) {
				stopLatch.countDown();
			}
		});
		container.start();

		template.setDefaultTopic(topic1);
		template.sendDefault(0, 0, "\"foo\"");
		template.sendDefault(0, 0, "\"bar\"");
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(data.get()).isEqualTo("bar");
		assertThat(recoverLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(failedGroupId.get()).isEqualTo("seekTestMaxFailures");

		props.put(ConsumerConfig.GROUP_ID_CONFIG, "seekTestMaxFailures.dlt");
		DefaultKafkaConsumerFactory<Integer, String> dltcf = new DefaultKafkaConsumerFactory<>(props);
		Consumer<Integer, String> consumer = dltcf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, topic1DLT);
		ConsumerRecord<Integer, String> dltRecord = KafkaTestUtils.getSingleRecord(consumer, topic1DLT);
		assertThat(dltRecord.value()).isEqualTo("foo");
		template.sendDefault(0, 0, "junkJson");
		dltRecord = KafkaTestUtils.getSingleRecord(consumer, topic1DLT);
		assertThat(dltRecord.value()).isEqualTo("junkJson");
		container.stop();
		pf.destroy();
		dltPf.destroy();
		consumer.close();
		assertThat(stopLatch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(errorHandler, times(4)).handle(any(), any(), any(), any());
		verify(errorHandler).clearThreadState();
	}

	@Test
	public void seekToCurrentErrorHandlerRecovers() {
		@SuppressWarnings("unchecked")
		BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer = mock(BiConsumer.class);
		SeekToCurrentErrorHandler eh = new SeekToCurrentErrorHandler(recoverer, 2);
		List<ConsumerRecord<?, ?>> records = new ArrayList<>();
		records.add(new ConsumerRecord<>("foo", 0, 0, null, "foo"));
		records.add(new ConsumerRecord<>("foo", 0, 1, null, "bar"));
		Consumer<?, ?> consumer = mock(Consumer.class);
		try {
			eh.handle(new RuntimeException(), records, consumer, null);
			fail("Expected exception");
		}
		catch (@SuppressWarnings("unused") KafkaException e) {
			// NOSONAR
		}
		verify(consumer).seek(new TopicPartition("foo", 0),  0L);
		verifyNoMoreInteractions(consumer);
		eh.handle(new RuntimeException(), records, consumer, null);
		verify(consumer).seek(new TopicPartition("foo", 0),  1L);
		verifyNoMoreInteractions(consumer);
		verify(recoverer).accept(eq(records.get(0)), any());
	}

	@Test
	public void seekToCurrentErrorHandlerRecoversManualAcksAsync() {
		seekToCurrentErrorHandlerRecoversManualAcks(false);
	}

	@Test
	public void seekToCurrentErrorHandlerRecoversManualAcksSync() {
		seekToCurrentErrorHandlerRecoversManualAcks(true);
	}

	private void seekToCurrentErrorHandlerRecoversManualAcks(boolean syncCommits) {
		@SuppressWarnings("unchecked")
		BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer = mock(BiConsumer.class);
		SeekToCurrentErrorHandler eh = new SeekToCurrentErrorHandler(recoverer, 2);
		eh.setCommitRecovered(true);
		List<ConsumerRecord<?, ?>> records = new ArrayList<>();
		records.add(new ConsumerRecord<>("foo", 0, 0, null, "foo"));
		records.add(new ConsumerRecord<>("foo", 1, 0, null, "bar"));
		Consumer<?, ?> consumer = mock(Consumer.class);
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		ContainerProperties properties = new ContainerProperties("foo");
		properties.setAckMode(AckMode.MANUAL_IMMEDIATE);
		properties.setSyncCommits(syncCommits);
		properties.setSyncCommitTimeout(Duration.ofSeconds(42));
		OffsetCommitCallback commitCallback = (offsets, ex) -> { };
		properties.setCommitCallback(commitCallback);
		given(container.getContainerProperties()).willReturn(properties);
		try {
			eh.handle(new RuntimeException(), records, consumer, container);
			fail("Expected exception");
		}
		catch (@SuppressWarnings("unused") KafkaException e) {
			// NOSONAR
		}
		verify(consumer).seek(new TopicPartition("foo", 0),  0L);
		verify(consumer).seek(new TopicPartition("foo", 1),  0L);
		verifyNoMoreInteractions(consumer);
		eh.handle(new RuntimeException(), records, consumer, container);
		verify(consumer, times(2)).seek(new TopicPartition("foo", 1),  0L);
		if (syncCommits) {
			verify(consumer)
					.commitSync(Collections.singletonMap(new TopicPartition("foo", 0), new OffsetAndMetadata(1L)),
							Duration.ofSeconds(42));
		}
		else {
			verify(consumer)
					.commitAsync(
							Collections.singletonMap(new TopicPartition("foo", 0), new OffsetAndMetadata(1L)),
							commitCallback);
		}
		verifyNoMoreInteractions(consumer);
		verify(recoverer).accept(eq(records.get(0)), any());
	}

	@Test
	public void testNeverRecover() {
		@SuppressWarnings("unchecked")
		BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer = mock(BiConsumer.class);
		SeekToCurrentErrorHandler eh = new SeekToCurrentErrorHandler(recoverer, -1);
		List<ConsumerRecord<?, ?>> records = new ArrayList<>();
		records.add(new ConsumerRecord<>("foo", 0, 0, null, "foo"));
		records.add(new ConsumerRecord<>("foo", 0, 1, null, "bar"));
		Consumer<?, ?> consumer = mock(Consumer.class);
		for (int i = 0; i < 20; i++) {
			try {
				eh.handle(new RuntimeException(), records, consumer, null);
				fail("Expected exception");
			}
			catch (@SuppressWarnings("unused") KafkaException e) {
				// NOSONAR
			}
		}
		verify(consumer, times(20)).seek(new TopicPartition("foo", 0),  0L);
		verifyNoMoreInteractions(consumer);
		verify(recoverer, never()).accept(any(), any());
	}

}
