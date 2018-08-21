/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.kafka.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.Serializer;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextStoppedEvent;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * The {@link ProducerFactory} implementation for a {@code singleton} shared
 * {@link Producer} instance.
 * <p>
 * This implementation will return the same {@link Producer} instance (if transactions are
 * not enabled) for the provided {@link Map} {@code configs} and optional {@link Serializer}
 * {@code keySerializer}, {@code valueSerializer} implementations on each
 * {@link #createProducer()} invocation.
 * <p>
 * The {@link Producer} is wrapped and the underlying {@link KafkaProducer} instance is
 * not actually closed when {@link Producer#close()} is invoked. The {@link KafkaProducer}
 * is physically closed when {@link DisposableBean#destroy()} is invoked or when the
 * application context publishes a {@link ContextStoppedEvent}. You can also invoke
 * {@link #reset()}.
 * <p>
 * Setting {@link #setTransactionIdPrefix(String)} enables transactions; in which case, a
 * cache of producers is maintained; closing a producer returns it to the cache. The
 * producers are closed and the cache is cleared when the factory is destroyed, the
 * application context stopped, or the {@link #reset()} method is called.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Murali Reddy
 * @author Nakul Mishra
 */
public class DefaultKafkaProducerFactory<K, V> implements ProducerFactory<K, V>, ApplicationContextAware,
		ApplicationListener<ContextStoppedEvent>, DisposableBean {

	private static final int DEFAULT_PHYSICAL_CLOSE_TIMEOUT = 30;

	private static final Log logger = LogFactory.getLog(DefaultKafkaProducerFactory.class);

	private final Map<String, Object> configs;

	private final AtomicInteger transactionIdSuffix = new AtomicInteger();

	private final BlockingQueue<CloseSafeProducer<K, V>> cache = new LinkedBlockingQueue<>();

	private volatile CloseSafeProducer<K, V> producer;

	private Serializer<K> keySerializer;

	private Serializer<V> valueSerializer;

	private int physicalCloseTimeout = DEFAULT_PHYSICAL_CLOSE_TIMEOUT;

	private String transactionIdPrefix;

	private ApplicationContext applicationContext;

	/**
	 * Construct a factory with the provided configuration.
	 * @param configs the configuration.
	 */
	public DefaultKafkaProducerFactory(Map<String, Object> configs) {
		this(configs, null, null);
	}

	/**
	 * Construct a factory with the provided configuration and {@link Serializer}s.
	 * @param configs the configuration.
	 * @param keySerializer the key {@link Serializer}.
	 * @param valueSerializer the value {@link Serializer}.
	 */
	public DefaultKafkaProducerFactory(Map<String, Object> configs,
			@Nullable Serializer<K> keySerializer,
			@Nullable Serializer<V> valueSerializer) {
		this.configs = new HashMap<>(configs);
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public void setKeySerializer(@Nullable Serializer<K> keySerializer) {
		this.keySerializer = keySerializer;
	}

	public void setValueSerializer(@Nullable Serializer<V> valueSerializer) {
		this.valueSerializer = valueSerializer;
	}

	/**
	 * The time to wait when physically closing the producer (when {@link #stop()} or {@link #destroy()} is invoked).
	 * Specified in seconds; default {@value #DEFAULT_PHYSICAL_CLOSE_TIMEOUT}.
	 * @param physicalCloseTimeout the timeout in seconds.
	 * @since 1.0.7
	 */
	public void setPhysicalCloseTimeout(int physicalCloseTimeout) {
		this.physicalCloseTimeout = physicalCloseTimeout;
	}

	/**
	 * Set the transactional.id prefix.
	 * @param transactionIdPrefix the prefix.
	 * @since 1.3
	 */
	public void setTransactionIdPrefix(String transactionIdPrefix) {
		Assert.notNull(transactionIdPrefix, "'transactionIdPrefix' cannot be null");
		this.transactionIdPrefix = transactionIdPrefix;
		enableIdempotentBehaviour();
	}

	/**
	 * When set to 'true', the producer will ensure that exactly one copy of each message is written in the stream.
	 */
	private void enableIdempotentBehaviour() {
		Object previousValue = this.configs.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
		if (logger.isDebugEnabled() && Boolean.FALSE.equals(previousValue)) {
			logger.debug("The '" + ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG +
					"' is set to false, may result in duplicate messages");
		}
	}

	/**
	 * Return an unmodifiable reference to the configuration map for this factory.
	 * Useful for cloning to make a similar factory.
	 * @return the configs.
	 * @since 1.3
	 */
	public Map<String, Object> getConfigurationProperties() {
		return Collections.unmodifiableMap(this.configs);
	}

	@Override
	public boolean transactionCapable() {
		return this.transactionIdPrefix != null;
	}

	@SuppressWarnings("resource")
	@Override
	public void destroy() throws Exception { //NOSONAR
		CloseSafeProducer<K, V> producer = this.producer;
		this.producer = null;
		if (producer != null) {
			producer.delegate.close(this.physicalCloseTimeout, TimeUnit.SECONDS);
		}
		producer = this.cache.poll();
		while (producer != null) {
			try {
				producer.delegate.close(this.physicalCloseTimeout, TimeUnit.SECONDS);
			}
			catch (Exception e) {
				logger.error("Exception while closing producer", e);
			}
			producer = this.cache.poll();
		}
	}

	@Override
	public void onApplicationEvent(ContextStoppedEvent event) {
		if (event.getApplicationContext().equals(this.applicationContext)) {
			reset();
		}
	}

	/**
	 * NoOp.
	 * @deprecated {@link org.springframework.context.Lifecycle} is no longer implemented.
	 */
	@Deprecated
	public void start() {
		// NOSONAR
	}

	/**
	 * NoOp.
	 * @deprecated {@link org.springframework.context.Lifecycle} is no longer implemented;
	 * use {@link #reset()} to close the {@link Producer}(s).
	 */
	@Deprecated
	public void stop() {
		reset();
	}

	/**
	 * Close the {@link Producer}(s) and clear the cache of transactional
	 * {@link Producer}(s).
	 * @since 2.2
	 */
	public void reset() {
		try {
			destroy();
		}
		catch (Exception e) {
			logger.error("Exception while closing producer", e);
		}
	}

	/**
	 * NoOp.
	 * @return always true.
	 * @deprecated {@link org.springframework.context.Lifecycle} is no longer implemented.
	 */
	@Deprecated
	public boolean isRunning() {
		return true;
	}

	@Override
	public Producer<K, V> createProducer() {
		if (this.transactionIdPrefix != null) {
			return createTransactionalProducer();
		}
		if (this.producer == null) {
			synchronized (this) {
				if (this.producer == null) {
					this.producer = new CloseSafeProducer<K, V>(createKafkaProducer());
				}
			}
		}
		return this.producer;
	}

	/**
	 * Subclasses must return a raw producer which will be wrapped in a
	 * {@link CloseSafeProducer}.
	 * @return the producer.
	 */
	protected Producer<K, V> createKafkaProducer() {
		return new KafkaProducer<K, V>(this.configs, this.keySerializer, this.valueSerializer);
	}

	/**
	 * Subclasses must return a producer from the {@link #getCache()} or a
	 * new raw producer wrapped in a {@link CloseSafeProducer}.
	 * @return the producer - cannot be null.
	 * @since 1.3
	 */
	protected Producer<K, V> createTransactionalProducer() {
		Producer<K, V> producer = this.cache.poll();
		if (producer == null) {
			Map<String, Object> configs = new HashMap<>(this.configs);
			configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
					this.transactionIdPrefix + this.transactionIdSuffix.getAndIncrement());
			producer = new KafkaProducer<K, V>(configs, this.keySerializer, this.valueSerializer);
			producer.initTransactions();
			return new CloseSafeProducer<K, V>(producer, this.cache);
		}
		else {
			return producer;
		}
	}

	protected BlockingQueue<CloseSafeProducer<K, V>> getCache() {
		return this.cache;
	}

	/**
	 * A wrapper class for the delegate.
	 *
	 * @param <K> the key type.
	 * @param <V> the value type.
	 *
	 */
	protected static class CloseSafeProducer<K, V> implements Producer<K, V> {

		private final Producer<K, V> delegate;

		private final BlockingQueue<CloseSafeProducer<K, V>> cache;

		private volatile boolean txFailed;

		CloseSafeProducer(Producer<K, V> delegate) {
			this(delegate, null);
			Assert.isTrue(!(delegate instanceof CloseSafeProducer), "Cannot double-wrap a producer");
		}

		CloseSafeProducer(Producer<K, V> delegate, BlockingQueue<CloseSafeProducer<K, V>> cache) {
			this.delegate = delegate;
			this.cache = cache;
		}

		@Override
		public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
			return this.delegate.send(record);
		}

		@Override
		public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
			return this.delegate.send(record, callback);
		}

		@Override
		public void flush() {
			this.delegate.flush();
		}

		@Override
		public List<PartitionInfo> partitionsFor(String topic) {
			return this.delegate.partitionsFor(topic);
		}

		@Override
		public Map<MetricName, ? extends Metric> metrics() {
			return this.delegate.metrics();
		}

		@Override
		public void initTransactions() {
			this.delegate.initTransactions();
		}

		@Override
		public void beginTransaction() throws ProducerFencedException {
			try {
				this.delegate.beginTransaction();
			}
			catch (RuntimeException e) {
				this.txFailed = true;
				throw e;
			}
		}

		@Override
		public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId)
				throws ProducerFencedException {
			this.delegate.sendOffsetsToTransaction(offsets, consumerGroupId);
		}

		@Override
		public void commitTransaction() throws ProducerFencedException {
			try {
				this.delegate.commitTransaction();
			}
			catch (RuntimeException e) {
				this.txFailed = true;
				throw e;
			}
		}

		@Override
		public void abortTransaction() throws ProducerFencedException {
			try {
				this.delegate.abortTransaction();
			}
			catch (RuntimeException e) {
				this.txFailed = true;
				throw e;
			}
		}

		@Override
		public void close() {
			if (this.cache != null) {
				if (this.txFailed) {
					logger.warn("Error during transactional operation; producer removed from cache; possible cause: "
							+ "broker restarted during transaction");

					this.delegate.close();
				}
				else {
					synchronized (this) {
						if (!this.cache.contains(this)) {
							this.cache.offer(this);
						}
					}
				}
			}
		}

		@Override
		public void close(long timeout, TimeUnit unit) {
			close();
		}

		@Override
		public String toString() {
			return "CloseSafeProducer [delegate=" + this.delegate + "]";
		}

	}

}
