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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer.ContainerOffsetResetStrategy;
import org.springframework.scheduling.SchedulingAwareRunnable;
import org.springframework.util.Assert;

/**
 * Single-threaded Message listener container using the Java {@link Consumer} supporting
 * auto-partition assignment or user-configured assignment.
 * <p>
 * With the latter, initial partition offsets can be provided.
 *
 * @author Gary Russell
 *
 */
public class KafkaMessageListenerContainer<K, V> extends AbstractMessageListenerContainer<K, V> {

	private final ConsumerFactory<K, V> consumerFactory;

	private final String[] topics;

	private final Pattern topicPattern;

	private final TopicPartition[] partitions;

	private ListenerConsumer listenerConsumer;

	private ContainerOffsetResetStrategy resetStrategy = ContainerOffsetResetStrategy.NONE;

	private long recentOffset = 1;

	private MessageListener<K, V> listener;

	private AcknowledgingMessageListener<K, V> acknowledgingMessageListener;

	/**
	 * Construct an instance with the supplied configuration properties and specific
	 * topics/partitions - when using this constructor, a
	 * {@code #setResetStrategy(ContainerOffsetResetStrategy)} can be used.
	 * @param consumerFactory the consumer factory.
	 * @param topicPartitions the topics/partitions; duplicates are eliminated.
	 */
	public KafkaMessageListenerContainer(ConsumerFactory<K, V> consumerFactory, TopicPartition... topicPartitions) {
		this(consumerFactory, null, null, topicPartitions);
	}

	/**
	 * Construct an instance with the supplied configuration properties and topics.
	 * When using this constructor, a
	 * {@code #setResetStrategy(ContainerOffsetResetStrategy)} cannot be used.
	 * @param consumerFactory the consumer factory.
	 * @param topics the topics.
	 */
	public KafkaMessageListenerContainer(ConsumerFactory<K, V> consumerFactory, String... topics) {
		this(consumerFactory, topics, null, null);
	}

	/**
	 * Construct an instance with the supplied configuration properties and topic
	 * pattern. When using this constructor, a
	 * {@code #setResetStrategy(ContainerOffsetResetStrategy)} cannot be used.
	 * @param consumerFactory the consumer factory.
	 * @param topicPattern the topic pattern.
	 */
	public KafkaMessageListenerContainer(ConsumerFactory<K, V> consumerFactory, Pattern topicPattern) {
		this(consumerFactory, null, topicPattern, null);
	}

	/**
	 * Construct an instance with the supplied configuration properties and topic
	 * pattern. Note: package protected - used by the ConcurrentMessageListenerContainer.
	 * @param consumerFactory the consumer factory.
	 * @param topics the topics.
	 * @param topicPattern the topic pattern.
	 * @param topicPartitions the topics/partitions; duplicates are eliminated.
	 */
	KafkaMessageListenerContainer(ConsumerFactory<K, V> consumerFactory, String[] topics, Pattern topicPattern,
			TopicPartition[] topicPartitions) {
		this.consumerFactory = consumerFactory;
		this.topics = topics;
		this.topicPattern = topicPattern;
		this.partitions = topicPartitions == null ? null : new LinkedHashSet<>(Arrays.asList(topicPartitions))
				.toArray(new TopicPartition[topicPartitions.length]);
	}

	/**
	 * The initial offset reset strategy, when explicit partitions are provided.
	 * <ul>
	 * <li>NONE: No reset</li>
	 * <li>EARLIEST: Set to the earliest message</li>
	 * <li>LATEST: Set to the last message; receive new messages only</li>
	 * <li>RECENT: Set to a recent message based on {@link #setRecentOffset(long) recentOffset}</li>
	 * </ul>
	 * @param resetStrategy the {@link ContainerOffsetResetStrategy}
	 */
	public void setResetStrategy(ContainerOffsetResetStrategy resetStrategy) {
		this.resetStrategy = resetStrategy;
	}

	/**
	 * Set the number of records back from the latest when using
	 * {@link ContainerOffsetResetStrategy#RECENT}.
	 * @param recentOffset the offset from the latest; default 1.
	 */
	public void setRecentOffset(long recentOffset) {
		this.recentOffset = recentOffset;
	}

	/**
	 * @return the {@link TopicPartition}s currently assigned to this container,
	 * either explicitly or by Kafka; may be null if not assigned yet.
	 */
	public Collection<TopicPartition> getAssignedPartitions() {
		if (this.listenerConsumer.definedPartitions != null) {
			return Collections.unmodifiableCollection(this.listenerConsumer.definedPartitions);
		}
		else if (this.listenerConsumer.assignedPartitions != null) {
			return Collections.unmodifiableCollection(this.listenerConsumer.assignedPartitions);
		}
		else {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void doStart() {
		if (isRunning()) {
			return;
		}
		setRunning(true);
		Object messageListener = getMessageListener();
		Assert.state(messageListener != null, "A MessageListener is required");
		if (messageListener instanceof AcknowledgingMessageListener) {
			this.acknowledgingMessageListener = (AcknowledgingMessageListener<K, V>) messageListener;
		}
		else if (messageListener instanceof MessageListener) {
			this.listener = (MessageListener<K, V>) messageListener;
		}
		else {
			throw new IllegalStateException("messageListener must be 'MessageListener' "
					+ "or 'AcknowledgingMessageListener', not " + messageListener.getClass().getName());
		}
		if (getTaskExecutor() == null) {
			setTaskExecutor(
					new SimpleAsyncTaskExecutor(getBeanName() == null ? "kafka-" : (getBeanName() + "-kafka-")));
		}
		this.listenerConsumer = new ListenerConsumer(this.listener, this.acknowledgingMessageListener,
				this.resetStrategy, this.recentOffset);
		getTaskExecutor().execute(this.listenerConsumer);
	}

	@Override
	protected void doStop() {
		if (isRunning()) {
			setRunning(false);
			this.listenerConsumer.consumer.wakeup();
		}
	}


	private class ListenerConsumer implements SchedulingAwareRunnable {

		private final Log logger = LogFactory.getLog(ListenerConsumer.class);

		private final CommitCallback callback = new CommitCallback();

		private final Consumer<K, V> consumer;

		private final ConcurrentMap<String, ConcurrentMap<Integer, Long>> manualOffsets = new ConcurrentHashMap<>();

		private final Map<String, Map<Integer, Long>> offsets = new HashMap<>();

		private final MessageListener<K, V> listener;

		private final AcknowledgingMessageListener<K, V> acknowledgingMessageListener;

		private final ContainerOffsetResetStrategy resetStrategy;

		private final long recentOffset;

		private final boolean autoCommit = KafkaMessageListenerContainer.this.consumerFactory.isAutoCommit();

		private Thread consumerThread;

		private volatile Collection<TopicPartition> definedPartitions;

		private volatile Collection<TopicPartition> assignedPartitions;

		public ListenerConsumer(MessageListener<K, V> listener, AcknowledgingMessageListener<K, V> ackListener,
				ContainerOffsetResetStrategy resetStrategy, long recentOffset) {
			Assert.state(!(getAckMode().equals(AckMode.MANUAL) || getAckMode().equals(AckMode.MANUAL_IMMEDIATE))
					|| !this.autoCommit,
					"Consumer cannot be configured for auto commit for ackMode " + getAckMode());
			Consumer<K, V> consumer = KafkaMessageListenerContainer.this.consumerFactory.createConsumer();
			ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {

				@Override
				public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
					KafkaMessageListenerContainer.this.logger.info("partitions revoked:" + partitions);
				}

				@Override
				public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
					ListenerConsumer.this.assignedPartitions = partitions;
					KafkaMessageListenerContainer.this.logger.info("partitions assigned:" + partitions);
				}

			};
			if (KafkaMessageListenerContainer.this.partitions == null) {
				if (KafkaMessageListenerContainer.this.topicPattern != null) {
					consumer.subscribe(KafkaMessageListenerContainer.this.topicPattern, rebalanceListener);
				}
				else {
					consumer.subscribe(Arrays.asList(KafkaMessageListenerContainer.this.topics), rebalanceListener);
				}
			}
			else {
				List<TopicPartition> topicPartitions = Arrays.asList(KafkaMessageListenerContainer.this.partitions);
				this.definedPartitions = topicPartitions;
				consumer.assign(topicPartitions);
			}
			this.consumer = consumer;
			this.listener = listener;
			this.acknowledgingMessageListener = ackListener;
			this.resetStrategy = resetStrategy;
			this.recentOffset = recentOffset;
		}

		@Override
		public boolean isLongLived() {
			return true;
		}

		@Override
		public void run() {
			this.consumerThread = Thread.currentThread();
			int count = 0;
			long last = System.currentTimeMillis();
			long now;
			if (isRunning() && this.definedPartitions != null) {
				initPartitionsIfNeeded();
			}
			final AckMode ackMode = getAckMode();
			while (isRunning()) {
				try {
					if (this.logger.isTraceEnabled()) {
						this.logger.trace("Polling...");
					}
					ConsumerRecords<K, V> records = this.consumer.poll(getPollTimeout());
					if (records != null) {
						count += records.count();
						if (this.logger.isDebugEnabled()) {
							this.logger.debug("Received: " + records.count() + " records");
						}
						Iterator<ConsumerRecord<K, V>> iterator = records.iterator();
						while (iterator.hasNext()) {
							final ConsumerRecord<K, V> record = iterator.next();
							invokeListener(record);
							if (!this.autoCommit && ackMode.equals(AckMode.RECORD)) {
								this.consumer.commitAsync(
										Collections.singletonMap(new TopicPartition(record.topic(), record.partition()),
												new OffsetAndMetadata(record.offset() + 1)), this.callback);
							}
						}
						if (!this.autoCommit) {
							if (ackMode.equals(AckMode.BATCH)) {
								if (!records.isEmpty()) {
									this.consumer.commitAsync(this.callback);
								}
							}
							else if (!ackMode.equals(AckMode.MANUAL_IMMEDIATE)) {
								if (!ackMode.equals(AckMode.MANUAL)) {
									updatePendingOffsets(records);
								}
								boolean countExceeded = count >= getAckCount();
								if (ackMode.equals(AckMode.COUNT) && countExceeded) {
									commitIfNecessary();
									count = 0;
								}
								else {
									now = System.currentTimeMillis();
									boolean elapsed = now - last > getAckTime();
									if (ackMode.equals(AckMode.TIME) && elapsed) {
										commitIfNecessary();
										last = now;
									}
									else if ((ackMode.equals(AckMode.COUNT_TIME) || ackMode.equals(AckMode.MANUAL))
											&& (elapsed || countExceeded)) {
										commitIfNecessary();
										last = now;
										count = 0;
									}
								}
							}
						}
					}
					else {
						if (this.logger.isDebugEnabled()) {
							this.logger.debug("No records");
						}
					}
				}
				catch (WakeupException e) {
					// No-op. Continue process
				}
				catch (Exception e) {
					if (getErrorHandler() != null) {
						getErrorHandler().handle(e, null);
					}
					else {
						this.logger.error("Container exception", e);
					}
				}
			}
			if (this.offsets.size() > 0) {
				commitIfNecessary();
			}
			try {
				this.consumer.unsubscribe();
			}
			catch (WakeupException e) {
				// No-op. Continue process
			}
			this.consumer.close();
			if (this.logger.isInfoEnabled()) {
				this.logger.info("Consumer stopped");
			}
		}

		private void invokeListener(final ConsumerRecord<K, V> record) {
			try {
				if (this.acknowledgingMessageListener != null) {
					this.acknowledgingMessageListener.onMessage(record, new Acknowledgment() {

						@Override
						public void acknowledge() {
							if (getAckMode().equals(AckMode.MANUAL)) {
								updateManualOffset(record);
							}
							else if (getAckMode().equals(AckMode.MANUAL_IMMEDIATE)) {
								if (Thread.currentThread().equals(ListenerConsumer.this.consumerThread)) {
									Map<TopicPartition, OffsetAndMetadata> commits = Collections.singletonMap(
											new TopicPartition(record.topic(), record.partition()),
											new OffsetAndMetadata(record.offset() + 1));
									if (ListenerConsumer.this.logger.isDebugEnabled()) {
										ListenerConsumer.this.logger.debug("Committing: " + commits);
									}
									ListenerConsumer.this.consumer.commitAsync(commits, ListenerConsumer.this.callback);
								}
								else {
									throw new IllegalStateException(
											"With MANUAL_IMMEDIATE ack mode, acknowledge() must be invoked on the "
													+ "consumer thread");
								}
							}
							else {
								throw new IllegalStateException("AckMode must be MANUAL or MANUAL_IMMEDIATE "
										+ "for manual acks");
							}
						}

					});
				}
				else {
					this.listener.onMessage(record);
				}
			}
			catch (Exception e) {
				if (getErrorHandler() != null) {
					getErrorHandler().handle(e, record);
				}
				else {
					this.logger.error("Listener threw an exception and no error handler for " + record, e);
				}
			}
		}

		private void initPartitionsIfNeeded() {
			/*
			 * Note: initial position setting is only supported with explicit topic assignment.
			 * When using auto assignment (subscribe), the ConsumerRebalanceListener is not
			 * called until we poll() the consumer.
			 */
			if (this.resetStrategy.equals(ContainerOffsetResetStrategy.EARLIEST)) {
				this.consumer.seekToBeginning(
						this.definedPartitions.toArray(new TopicPartition[this.definedPartitions.size()]));
			}
			else if (this.resetStrategy.equals(ContainerOffsetResetStrategy.LATEST)) {
				this.consumer.seekToEnd(
						this.definedPartitions.toArray(new TopicPartition[this.definedPartitions.size()]));
			}
			else if (this.resetStrategy.equals(ContainerOffsetResetStrategy.RECENT)) {
				this.consumer.seekToEnd(
						this.definedPartitions.toArray(new TopicPartition[this.definedPartitions.size()]));
				for (TopicPartition topicPartition : this.definedPartitions) {
					long newOffset = this.consumer.position(topicPartition) - this.recentOffset;
					this.consumer.seek(topicPartition, newOffset);
					if (this.logger.isDebugEnabled()) {
						this.logger.debug("Reset " + topicPartition + " to offset " + newOffset);
					}
				}
			}
		}

		private void updatePendingOffsets(ConsumerRecords<K, V> records) {
			for (ConsumerRecord<K, V> record : records) {
				if (!this.offsets.containsKey(record.topic())) {
					this.offsets.put(record.topic(), new HashMap<Integer, Long>());
				}
				this.offsets.get(record.topic()).put(record.partition(), record.offset());
			}
		}

		private void updateManualOffset(ConsumerRecord<K, V> record) {
			if (!this.manualOffsets.containsKey(record.topic())) {
				this.manualOffsets.putIfAbsent(record.topic(), new ConcurrentHashMap<Integer, Long>());
			}
			this.manualOffsets.get(record.topic()).put(record.partition(), record.offset());
		}

		private void commitIfNecessary() {
			Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
			if (AckMode.MANUAL.equals(getAckMode())) {
				for (Entry<String, ConcurrentMap<Integer, Long>> entry : this.manualOffsets.entrySet()) {
					Iterator<Entry<Integer, Long>> iterator = entry.getValue().entrySet().iterator();
					while (iterator.hasNext()) {
						Entry<Integer, Long> offset = iterator.next();
						commits.put(new TopicPartition(entry.getKey(), offset.getKey()),
								new OffsetAndMetadata(offset.getValue() + 1));
						iterator.remove();
					}
				}
			}
			else {
				for (Entry<String, Map<Integer, Long>> entry : this.offsets.entrySet()) {
					for (Entry<Integer, Long> offset : entry.getValue().entrySet()) {
						commits.put(new TopicPartition(entry.getKey(), offset.getKey()),
								new OffsetAndMetadata(offset.getValue() + 1));
					}
				}
			}
			this.offsets.clear();
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Committing: " + commits);
			}
			if (!commits.isEmpty()) {
				this.consumer.commitAsync(commits, this.callback);
			}
		}
	}

	private static final class CommitCallback implements OffsetCommitCallback {

		private static final Log logger = LogFactory.getLog(OffsetCommitCallback.class);

		@Override
		public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
			if (exception != null) {
				logger.error("Commit failed for " + offsets, exception);
			}
			else if (logger.isDebugEnabled()) {
				logger.debug("Commits for " + offsets + " completed");
			}
		}

	}

}
