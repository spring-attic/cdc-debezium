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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.connect.source.SourceRecord;
import reactor.core.publisher.FluxSink;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.StreamMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeTypeUtils;

/**
 * @author Christian Tzolov
 */
@Configuration
@EnableConfigurationProperties({ CdcStreamProperties.class })
@Import({ CdcCommonConfiguration.class })
public class CdcStreamConfiguration {

	private static final Log logger = LogFactory.getLog(CdcStreamConfiguration.class);

	@Bean
	public Consumer<FluxSink<Message<?>>> engine(EmbeddedEngine.Builder embeddedEngineBuilder,
			Function<SourceRecord, byte[]> valueSerializer, Function<SourceRecord, byte[]> keySerializer,
			Function<SourceRecord, SourceRecord> recordFlattering,
			ObjectMapper mapper, CdcStreamProperties cdcStreamingEngineProperties) {

		return emitter -> {

			Consumer<SourceRecord> messageConsumer = sourceRecord -> {

				// When the Tombstone events are disabled, the engine still sends a record but serialized to null
				if (sourceRecord == null) {
					logger.debug("Ignore disabled tombstone events");
					return;
				}

				Object cdcJsonPayload = valueSerializer.apply(sourceRecord);

				// When the tombstone event is enabled, Debezium serializes the payload to null (e.g. empty payload)
				// while the metadata information is carried through the headers (cdc.key)
				if ((cdcJsonPayload == null) && KafkaPresent.isKafkaPresent()) {
					cdcJsonPayload = KafkaPresent.getKafkaNullInstance();
				}

				// If payload is still null ignore the message.
				if (cdcJsonPayload == null) {
					logger.warn("dropped null payload message. Is kafka present:" + KafkaPresent.isKafkaPresent());
					return;
				}

				MessageBuilder<?> messageBuilder = MessageBuilder
						.withPayload(cdcJsonPayload)
						.setHeader("cdc.topic", sourceRecord.topic())
						.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE);

				if (cdcStreamingEngineProperties.getHeader().isKey()) {
					messageBuilder.setHeader("cdc.key", new String(keySerializer.apply(sourceRecord)));
				}

				if (cdcStreamingEngineProperties.getHeader().isOffset()) {
					try {
						messageBuilder.setHeader("cdc.offset", mapper.writeValueAsString(sourceRecord.sourceOffset()));
					}
					catch (JsonProcessingException e) {
						logger.warn("Failed to record cdc.offset header", e);
					}
				}

				emitter.next(messageBuilder.build());
			};

			EmbeddedEngine engine = embeddedEngineBuilder
					.notifying(record -> messageConsumer.accept(recordFlattering.apply(record)))
					.build();

			ExecutorService executor = Executors.newSingleThreadExecutor();
			executor.execute(engine);

			emitter.onDispose(() -> {
				engine.stop();
				executor.shutdown();
			});
		};
	}
	
	@Bean
	@StreamMessageConverter
	@ConditionalOnClass(name = "org.springframework.kafka.support.KafkaNull")
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
