/*
 * Copyright 2019 the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.cdc.common.core;

import java.util.Collections;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.cloud.stream.annotation.StreamMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.MessageConverter;

/**
 * @author Christian Tzolov
 */
@Configuration
public class CdcTombstoneConfiguration {

	public static final String ORG_SPRINGFRAMEWORK_KAFKA_SUPPORT_KAFKA_NULL = "org.springframework.kafka.support.KafkaNull";

	public interface TombstoneSupport {
		boolean isKafkaPresent();
		Object getTombstonePayload();
	}

	/**
	 * When Kafka is present on the classpath, use the KafkaNull.INSTANCE as tombstone message payload.
	 */
	@Bean
	@ConditionalOnClass(name = ORG_SPRINGFRAMEWORK_KAFKA_SUPPORT_KAFKA_NULL)
	public TombstoneSupport kafkaTombstoneHelper() {
		return new TombstoneSupport() {
			@Override
			public boolean isKafkaPresent() {
				return true;
			}

			@Override
			public Object getTombstonePayload() {
				return KafkaNull.INSTANCE;
			}
		};
	}

	@Bean
	@StreamMessageConverter
	@ConditionalOnClass(name = ORG_SPRINGFRAMEWORK_KAFKA_SUPPORT_KAFKA_NULL)
	public MessageConverter kafkaNullConverter() {
		class KafkaNullConverter extends AbstractMessageConverter {

			KafkaNullConverter() {
				super(Collections.emptyList());
			}

			@Override
			protected boolean supports(Class<?> clazz) {
				return KafkaNull.class.equals(clazz);
			}

			@Override
			protected Object convertFromInternal(Message<?> message, Class<?> targetClass, Object conversionHint) {
				return message.getPayload();
			}

			@Override
			protected Object convertToInternal(Object payload, MessageHeaders headers, Object conversionHint) {
				return payload;
			}

		}
		return new KafkaNullConverter();
	}
}
