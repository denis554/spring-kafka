/*
 * Copyright 2019 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;

/**
 * Provides a method-chaining way to build {@link org.apache.kafka.streams.kstream.KStream#branch branches} in
 * Kafka Streams processor topology.
 * <p>
 * Example of usage:
 * <pre>
 * {@code
 * new KafkaStreamsBrancher<String, String>()
 *    .branch((key, value) -> value.contains("A"), ks->ks.to("A"))
 *    .branch((key, value) -> value.contains("B"), ks->ks.to("B"))
 *    //default branch should not necessarily be defined in the end
 *    .defaultBranch(ks->ks.to("C"))
 *    .onTopOf(builder.stream("source"))
 * }
 * </pre>
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 *
 * @author Ivan Ponomarev
 * @author Artem Bilan
 *
 * @since 2.2.4
 */
public final class KafkaStreamBrancher<K, V> {

	private final List<Predicate<? super K, ? super V>> predicateList = new ArrayList<>();

	private final List<Consumer<? super KStream<K, V>>> consumerList = new ArrayList<>();

	private Consumer<? super KStream<K, V>> defaultConsumer;

	/**
	 * Defines a new branch.
	 * @param predicate {@link Predicate} instance
	 * @param consumer  The consumer of this branch's {@code KStream}
	 * @return {@code this}
	 */
	public KafkaStreamBrancher<K, V> branch(Predicate<? super K, ? super V> predicate,
			Consumer<? super KStream<K, V>> consumer) {
		this.predicateList.add(Objects.requireNonNull(predicate));
		this.consumerList.add(Objects.requireNonNull(consumer));
		return this;
	}

	/**
	 * Defines a default branch. All the messages that were not dispatched to other branches will be directed
	 * to this stream. This method should not necessarily be called in the end
	 * of chain.
	 * @param consumer The consumer of this branch's {@code KStream}
	 * @return {@code this}
	 */
	public KafkaStreamBrancher<K, V> defaultBranch(Consumer<? super KStream<K, V>> consumer) {
		this.defaultConsumer = Objects.requireNonNull(consumer);
		return this;
	}

	/**
	 * Terminating method that builds branches on top of given {@code KStream}.
	 * @param stream {@code KStream} to split
	 * @return the provided stream
	 */
	public KStream<K, V> onTopOf(KStream<K, V> stream) {
		if (this.defaultConsumer != null) {
			this.predicateList.add((k, v) -> true);
			this.consumerList.add(this.defaultConsumer);
		}
		@SuppressWarnings({ "unchecked", "rawtypes" })
		Predicate<? super K, ? super V>[] predicates = this.predicateList.toArray(new Predicate[0]);
		KStream<K, V>[] result = stream.branch(predicates);
		for (int i = 0; i < this.consumerList.size(); i++) {
			this.consumerList.get(i).accept(result[i]);
		}
		return stream;
	}

}
