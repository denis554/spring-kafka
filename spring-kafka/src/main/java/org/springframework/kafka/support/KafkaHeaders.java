/*
 * Copyright 2014-2016 the original author or authors.
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

package org.springframework.kafka.support;

/**
 * The Kafka specific message headers constants.
 *
 * @author Artem Bilan
 * @author Marius Bogoevici
 */
public abstract class KafkaHeaders {

	private static final String PREFIX = "kafka_";

	/**
	 * The header for topic.
	 */
	public static final String TOPIC = PREFIX + "topic";


	/**
	 * The header for message key.
	 */
	public static final String MESSAGE_KEY = PREFIX + "messageKey";

	/**
	 * The header for topic partition.
	 */
	public static final String PARTITION_ID = PREFIX + "partitionId";

	/**
	 * The header for partition offset.
	 */
	public static final String OFFSET = PREFIX + "offset";

	/**
	 * The header for {@link Acknowledgment}.
	 */
	public static final String ACKNOWLEDGMENT = PREFIX + "acknowledgment";

}
