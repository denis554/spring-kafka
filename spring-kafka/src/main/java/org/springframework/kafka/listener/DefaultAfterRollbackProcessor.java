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

package org.springframework.kafka.listener;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Default implementation of {@link AfterRollbackProcessor}. Seeks all
 * topic/partitions so the records will be re-fetched, including the failed
 * record.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 * @since 1.3.5
 *
 */
public class DefaultAfterRollbackProcessor<K, V> implements AfterRollbackProcessor<K, V> {

	private static final Log logger = LogFactory.getLog(DefaultAfterRollbackProcessor.class);

	@Override
	public void process(List<ConsumerRecord<K, V>> records, Consumer<K, V> consumer) {
		Map<TopicPartition, Long> partitions = new HashMap<>();
		records.forEach(r -> partitions.computeIfAbsent(new TopicPartition(r.topic(), r.partition()),
				offset -> r.offset()));
		partitions.forEach((topicPartition, offset) -> {
			try {
				consumer.seek(topicPartition, offset);
			}
			catch (Exception e) {
				logger.error("Failed to seek " + topicPartition + " to " + offset);
			}
		});
	}

}
