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

package org.springframework.kafka.transaction;

import org.springframework.data.transaction.ChainedTransactionManager;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.Assert;

/**
 * A {@link ChainedTransactionManager} that has exactly one
 * {@link KafkaAwareTransactionManager} in the chain.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @since 2.1.3
 *
 */
public class ChainedKafkaTransactionManager<K, V> extends ChainedTransactionManager implements KafkaAwareTransactionManager<K, V> {

	private final KafkaAwareTransactionManager<K, V> kafkaTransactionManager;

	@SuppressWarnings("unchecked")
	public ChainedKafkaTransactionManager(PlatformTransactionManager... transactionManagers) {
		super(transactionManagers);
		KafkaAwareTransactionManager<K, V> kafkaTransactionManager = null;
		for (PlatformTransactionManager tm : transactionManagers) {
			if (tm instanceof KafkaAwareTransactionManager) {
				Assert.isNull(kafkaTransactionManager, "Only one KafkaAwareTransactionManager is allowed");
				kafkaTransactionManager = (KafkaTransactionManager<K, V>) tm;
			}
		}
		Assert.notNull(kafkaTransactionManager, "Exactly one KafkaAwareTransactionManager is required");
		this.kafkaTransactionManager = kafkaTransactionManager;
	}

	@Override
	public ProducerFactory<K, V> getProducerFactory() {
		return this.kafkaTransactionManager.getProducerFactory();
	}

}
