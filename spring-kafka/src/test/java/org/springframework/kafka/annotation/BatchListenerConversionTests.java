/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.kafka.annotation;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Gary Russell
 * @since 1.3.2
 *
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public class BatchListenerConversionTests {

	private static final String DEFAULT_TEST_GROUP_ID = "blc";

	@ClassRule // one topic to preserve order
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 1, "blc1", "blc2");

	@Autowired
	private Config config;

	@Autowired
	private KafkaTemplate<Integer, Foo> template;

	@Test
	public void testBatchOfPojos() throws Exception {
		doTest(this.config.listener1(), "blc1");
		doTest(this.config.listener2(), "blc2");
	}

	private void doTest(Listener listener, String topic) throws InterruptedException {
		this.template.send(new GenericMessage<>(
				new Foo("bar"), Collections.singletonMap(KafkaHeaders.TOPIC, topic)));
		this.template.send(new GenericMessage<>(
				new Foo("baz"), Collections.singletonMap(KafkaHeaders.TOPIC, topic)));
		assertThat(listener.latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(listener.latch2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(listener.received.size()).isGreaterThan(0);
		assertThat(listener.received.get(0)).isInstanceOf(Foo.class);
		assertThat(listener.received.get(0).bar).isEqualTo("bar");
		assertThat((listener.receivedTopics).get(0)).isEqualTo(topic);
		assertThat((listener.receivedPartitions).get(0)).isEqualTo(0);
	}

	@Configuration
	@EnableKafka
	public static class Config {

		@Bean
		public KafkaListenerContainerFactory<?> kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			factory.setBatchListener(true);
			factory.setMessageConverter(new BatchMessagingMessageConverter(converter()));
			return factory;
		}

		@Bean
		public DefaultKafkaConsumerFactory<Integer, String> consumerFactory() {
			return new DefaultKafkaConsumerFactory<>(consumerConfigs());
		}

		@Bean
		public Map<String, Object> consumerConfigs() {
			Map<String, Object> consumerProps =
					KafkaTestUtils.consumerProps(DEFAULT_TEST_GROUP_ID, "false", embeddedKafka);
			consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			return consumerProps;
		}

		@Bean
		public KafkaTemplate<Integer, Foo> template() {
			KafkaTemplate<Integer, Foo> kafkaTemplate = new KafkaTemplate<>(producerFactory());
			kafkaTemplate.setMessageConverter(converter());
			return kafkaTemplate;
		}

		@Bean
		public StringJsonMessageConverter converter() {
			return new StringJsonMessageConverter();
		}

		@Bean
		public ProducerFactory<Integer, Foo> producerFactory() {
			return new DefaultKafkaProducerFactory<>(producerConfigs());
		}

		@Bean
		public Map<String, Object> producerConfigs() {
			return KafkaTestUtils.producerProps(embeddedKafka);
		}

		@Bean
		public Listener listener1() {
			return new Listener("blc1");
		}

		@Bean
		public Listener listener2() {
			return new Listener("blc2");
		}

	}

	public static class Listener {

		private final String topic;

		private final CountDownLatch latch1 = new CountDownLatch(1);

		private final CountDownLatch latch2 = new CountDownLatch(1);

		private List<Foo> received;

		private List<String> receivedTopics;

		private List<Integer> receivedPartitions;

		public Listener(String topic) {
			this.topic = topic;
		}

		@KafkaListener(topics = "#{__listener.topic}", groupId = "#{__listener.topic}.group")
		public void listen1(List<Foo> foos, @Header(KafkaHeaders.RECEIVED_TOPIC) List<String> topics,
				@Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Integer> partitions) {
			if (this.received == null) {
				this.received = foos;
			}
			this.receivedTopics = topics;
			this.receivedPartitions = partitions;
			this.latch1.countDown();
		}

		@KafkaListener(beanRef = "__x", topics = "#{__x.topic}", groupId = "#{__x.topic}.group2")
		public void listen2(List<Foo> foos) {
			this.latch2.countDown();
		}

		public String getTopic() {
			return this.topic;
		}

	}

	public static class Foo {

		public String bar;

		public Foo() {
			super();
		}

		public Foo(String bar) {
			this.bar = bar;
		}

		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

		@Override
		public String toString() {
			return "Foo [bar=" + this.bar + "]";
		}

	}

}
