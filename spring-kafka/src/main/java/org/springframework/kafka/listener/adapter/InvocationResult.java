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

package org.springframework.kafka.listener.adapter;

import org.springframework.expression.Expression;

/**
 * The result of a method invocation.
 *
 * @author Gary Russell
 * @since 2.2
 */
public final class InvocationResult {

	private final Object result;

	private final Expression sendTo;

	private final boolean messageReturnType;

	public InvocationResult(Object result, Expression sendTo, boolean messageReturnType) {
		this.result = result;
		this.sendTo = sendTo;
		this.messageReturnType = messageReturnType;
	}

	public Object getResult() {
		return this.result;
	}

	public Expression getSendTo() {
		return this.sendTo;
	}

	public boolean isMessageReturnType() {
		return this.messageReturnType;
	}

	@Override
	public String toString() {
		return this.result.toString();
	}

}
