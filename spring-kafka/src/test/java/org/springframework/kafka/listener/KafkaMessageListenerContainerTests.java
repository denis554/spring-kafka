/*
 * Copyright 2016 the original author or authors.
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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.listener.adapter.RetryingMessageListenerAdapter;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

/**
 * Tests for the listener container.
 *
 * @author Gary Russell
 * @author Martin Dam
 */
public class KafkaMessageListenerContainerTests {

	private final Log logger = LogFactory.getLog(this.getClass());

	private static String topic1 = "testTopic1";

	private static String topic2 = "testTopic2";

	private static String topic3 = "testTopic3";

	private static String topic4 = "testTopic4";

	private static String topic5 = "testTopic5";

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, topic1, topic2, topic3, topic4, topic5);

	@Rule
	public TestName testName = new TestName();

	@Test
	public void testSlowListener() throws Exception {
		logger.info("Start " + this.testName.getMethodName());
		Map<String, Object> props = KafkaTestUtils.consumerProps("slow1", "false", embeddedKafka);
//		props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 6); // 2 per poll
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ContainerProperties containerProps = new ContainerProperties(topic1);
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		final CountDownLatch latch = new CountDownLatch(6);
		final BitSet bitSet = new BitSet(6);
		containerProps.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				logger.info("slow1: " + message);
				bitSet.set((int) (message.partition() * 3 + message.offset()));
				try {
					Thread.sleep(1000);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				latch.countDown();
			}

		});
		container.setBeanName("testSlow1");
		containerProps.setPauseAfter(100);
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Consumer<?, ?> consumer = spyOnConsumer(container);
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic1);
		template.sendDefault(0, "foo");
		template.sendDefault(2, "bar");
		template.sendDefault(0, "baz");
		template.sendDefault(2, "qux");
		template.flush();
		Thread.sleep(300);
		template.sendDefault(0, "fiz");
		template.sendDefault(2, "buz");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(bitSet.cardinality()).isEqualTo(6);
		verify(consumer, atLeastOnce()).pause(any(TopicPartition.class), any(TopicPartition.class));
		verify(consumer, atLeastOnce()).resume(any(TopicPartition.class), any(TopicPartition.class));
		container.stop();
		logger.info("Stop " + this.testName.getMethodName());
	}

	@Test
	public void testSlowListenerManualCommit() throws Exception {
		testSlowListenerManualGuts(AckMode.MANUAL_IMMEDIATE_SYNC, topic2);
		// to be sure the commits worked ok so run the tests again and the second tests start at the committed offset.
		testSlowListenerManualGuts(AckMode.MANUAL_IMMEDIATE_SYNC, topic2);
	}

	private void testSlowListenerManualGuts(AckMode ackMode, String topic) throws Exception {
		logger.info("Start " + this.testName.getMethodName() + ackMode);
		Map<String, Object> props = KafkaTestUtils.consumerProps("slow2", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ContainerProperties containerProps = new ContainerProperties(topic);
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		final CountDownLatch latch = new CountDownLatch(6);
		final BitSet bitSet = new BitSet(4);
		containerProps.setMessageListener(new AcknowledgingMessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message, Acknowledgment ack) {
				logger.info("slow2: " + message);
				bitSet.set((int) (message.partition() * 3 + message.offset()));
				try {
					Thread.sleep(1000);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				ack.acknowledge();
				latch.countDown();
			}

		});
		container.setBeanName("testSlow2");
		containerProps.setPauseAfter(100);
		containerProps.setAckMode(ackMode);
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Consumer<?, ?> consumer = spyOnConsumer(container);
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic);
		template.sendDefault(0, "foo");
		template.sendDefault(2, "bar");
		template.sendDefault(0, "baz");
		template.sendDefault(2, "qux");
		template.flush();
		Thread.sleep(300);
		template.sendDefault(0, "fiz");
		template.sendDefault(2, "buz");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(bitSet.cardinality()).isEqualTo(6);
		verify(consumer, atLeastOnce()).pause(any(TopicPartition.class), any(TopicPartition.class));
		verify(consumer, atLeastOnce()).resume(any(TopicPartition.class), any(TopicPartition.class));
		container.stop();
		logger.info("Stop " + this.testName.getMethodName() + ackMode);
	}

	@Test
	public void testSlowConsumerCommitsAreProcessed() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps("slow", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic5);
		containerProps.setAckCount(1);
		containerProps.setPauseAfter(100);
		containerProps.setAckMode(AckMode.MANUAL);
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		final CountDownLatch latch = new CountDownLatch(3);
		containerProps.setMessageListener((AcknowledgingMessageListener<Integer, String>) (message, ack) -> {
			logger.info("slow: " + message);
			try {
				Thread.sleep(1000);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			ack.acknowledge();
			latch.countDown();
		});
		container.setBeanName("testSlow");
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Consumer<?, ?> consumer = spyOnConsumer(container);


		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic5);
		template.sendDefault(0, "foo");
		template.sendDefault(2, "bar");
		template.flush();
		Thread.sleep(300);
		template.sendDefault(0, "fiz");
		template.sendDefault(2, "buz");
		template.flush();

		// Verify that commitSync is called when paused
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		verify(consumer, atLeastOnce()).pause(any(TopicPartition.class), any(TopicPartition.class));
		verify(consumer, atLeastOnce()).commitSync(any());
		verify(consumer, atLeastOnce()).resume(any(TopicPartition.class), any(TopicPartition.class));
		container.stop();
	}

	@Test
	public void testSlowConsumerWithException() throws Exception {
		logger.info("Start " + this.testName.getMethodName());
		Map<String, Object> props = KafkaTestUtils.consumerProps("slow3", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ContainerProperties containerProps = new ContainerProperties(topic3);
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		final CountDownLatch latch = new CountDownLatch(18);
		final BitSet bitSet = new BitSet(6);
		final Map<String, AtomicInteger> faults = new HashMap<>();
		RetryingMessageListenerAdapter<Integer, String> adapter = new RetryingMessageListenerAdapter<>(
					new MessageListener<Integer, String>() {

				@Override
				public void onMessage(ConsumerRecord<Integer, String> message) {
					logger.info("slow3: " + message);
					bitSet.set((int) (message.partition() * 3 + message.offset()));
					String key = message.topic() + message.partition() + message.offset();
					if (faults.get(key) == null) {
						faults.put(key, new AtomicInteger(1));
					}
					else {
						faults.get(key).incrementAndGet();
					}
					latch.countDown(); // 3 per = 18
					if (faults.get(key).get() < 3) { // succeed on the third attempt
						throw new FooEx();
					}
				}

			}, buildRetry(), null);
		containerProps.setMessageListener(adapter);
		containerProps.setPauseAfter(100);
		container.setBeanName("testSlow3");
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Consumer<?, ?> consumer = spyOnConsumer(container);
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic3);
		template.sendDefault(0, "foo");
		template.sendDefault(2, "bar");
		template.sendDefault(0, "baz");
		template.sendDefault(2, "qux");
		template.flush();
		Thread.sleep(300);
		template.sendDefault(0, "fiz");
		template.sendDefault(2, "buz");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(bitSet.cardinality()).isEqualTo(6);
		verify(consumer, atLeastOnce()).pause(any(TopicPartition.class), any(TopicPartition.class));
		verify(consumer, atLeastOnce()).resume(any(TopicPartition.class), any(TopicPartition.class));
		container.stop();
		logger.info("Stop " + this.testName.getMethodName());
	}

	@Test
	public void testSlowConsumerWithSlowThenExceptionThenGood() throws Exception {
		logger.info("Start " + this.testName.getMethodName());
		Map<String, Object> props = KafkaTestUtils.consumerProps("slow4", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ContainerProperties containerProps = new ContainerProperties(topic4);
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		final CountDownLatch latch = new CountDownLatch(18);
		final BitSet bitSet = new BitSet(6);
		final Map<String, AtomicInteger> faults = new HashMap<>();
		RetryingMessageListenerAdapter<Integer, String> adapter = new RetryingMessageListenerAdapter<>(
			new MessageListener<Integer, String>() {

				@Override
				public void onMessage(ConsumerRecord<Integer, String> message) {
					logger.info("slow4: " + message);
					bitSet.set((int) (message.partition() * 4 + message.offset()));
					String key = message.topic() + message.partition() + message.offset();
					if (faults.get(key) == null) {
						faults.put(key, new AtomicInteger(1));
					}
					else {
						faults.get(key).incrementAndGet();
					}
					latch.countDown(); // 3 per = 18
					if (faults.get(key).get() == 1) {
						try {
							Thread.sleep(1000);
						}
						catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
					}
					if (faults.get(key).get() < 3) { // succeed on the third attempt
						throw new FooEx();
					}
				}

			}, buildRetry(), null);
		containerProps.setMessageListener(adapter);
		containerProps.setPauseAfter(100);
		container.setBeanName("testSlow4");
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Consumer<?, ?> consumer = spyOnConsumer(container);
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic4);
		template.sendDefault(0, "foo");
		template.sendDefault(2, "bar");
		template.sendDefault(0, "baz");
		template.sendDefault(2, "qux");
		template.flush();
		Thread.sleep(300);
		template.sendDefault(0, "fiz");
		template.sendDefault(2, "buz");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(bitSet.cardinality()).isEqualTo(6);
		verify(consumer, atLeastOnce()).pause(any(TopicPartition.class), any(TopicPartition.class));
		verify(consumer, atLeastOnce()).resume(any(TopicPartition.class), any(TopicPartition.class));
		container.stop();
		logger.info("Stop " + this.testName.getMethodName());
	}

	private RetryTemplate buildRetry() {
		SimpleRetryPolicy policy = new SimpleRetryPolicy(3, Collections.singletonMap(FooEx.class, true));
		RetryTemplate retryTemplate = new RetryTemplate();
		retryTemplate.setRetryPolicy(policy);
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		retryTemplate.setBackOffPolicy(backOffPolicy);
		return retryTemplate;
	}

	private Consumer<?, ?> spyOnConsumer(KafkaMessageListenerContainer<Integer, String> container) {
		Consumer<?, ?> consumer = spy(
				KafkaTestUtils.getPropertyValue(container, "listenerConsumer.consumer", Consumer.class));
		new DirectFieldAccessor(KafkaTestUtils.getPropertyValue(container, "listenerConsumer"))
				.setPropertyValue("consumer", consumer);
		return consumer;
	}

	@SuppressWarnings("serial")
	public static class FooEx extends RuntimeException {

	}

}
