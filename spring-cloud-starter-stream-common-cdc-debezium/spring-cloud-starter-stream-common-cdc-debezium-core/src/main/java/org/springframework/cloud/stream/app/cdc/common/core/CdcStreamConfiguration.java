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

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeTypeUtils;

/**
 * @author Christian Tzolov
 */
@Configuration
@EnableConfigurationProperties({ CdcStreamProperties.class })
@Import({ CdcCommonConfiguration.class, CdcTombstoneConfiguration.class })
public class CdcStreamConfiguration {

	private static final Log logger = LogFactory.getLog(CdcStreamConfiguration.class);

	@Bean
	public Consumer<FluxSink<Message<?>>> engine(EmbeddedEngine.Builder embeddedEngineBuilder,
			Function<SourceRecord, byte[]> valueSerializer, Function<SourceRecord, byte[]> keySerializer,
			Function<SourceRecord, SourceRecord> recordFlattering,
			ObjectMapper mapper, CdcStreamProperties cdcStreamingEngineProperties,
			CdcTombstoneConfiguration.TombstoneSupport tombstoneSupport) {

		return emitter -> {

			Consumer<SourceRecord> messageConsumer = sourceRecord -> {

				// When cdc.flattering.deleteHandlingMode=none and cdc.flattering.dropTombstones=false
				// then on deletion event an additional sourceRecord is sent with value Null.
				// Here we filter out such condition.
				if (sourceRecord == null) {
					logger.debug("Ignore disabled tombstone events");
					return;
				}

				Object cdcJsonPayload = valueSerializer.apply(sourceRecord);

				// When the tombstone event is enabled, Debezium serializes the payload to null (e.g. empty payload)
				// while the metadata information is carried through the headers (cdc_key).
				// Note: Event for none flattered responses, when the cdc.config.tombstones.on.delete=true (default),
				// tombstones are generate by Debezium and handled by the code below.
				if ((cdcJsonPayload == null) && tombstoneSupport.isKafkaPresent()) {
					cdcJsonPayload = tombstoneSupport.getTombstonePayload();
				}

				// If payload is still null ignore the message.
				if (cdcJsonPayload == null) {
					logger.warn("dropped null payload message. Is kafka present:" + tombstoneSupport.isKafkaPresent());
					return;
				}

				MessageBuilder<?> messageBuilder = MessageBuilder
						.withPayload(cdcJsonPayload)
						.setHeader("cdc_key", new String(keySerializer.apply(sourceRecord)))
						.setHeader("cdc_topic", sourceRecord.topic())
						.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE);

				// When cdc.flattering.operationHeader=true
				if (sourceRecord.headers() != null
						&& sourceRecord.headers().lastWithName("__debezium-operation") != null) {
					messageBuilder.setHeader("cdc_operation",
							sourceRecord.headers().lastWithName("__debezium-operation").value().toString());
				}

				if (cdcStreamingEngineProperties.getHeader().isOffset()) {
					try {
						messageBuilder.setHeader("cdc_offset",
								mapper.writeValueAsString(sourceRecord.sourceOffset()));
					}
					catch (JsonProcessingException e) {
						logger.warn("Failed to record cdc_offset header", e);
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

	/**
	 * The default implementation effectively ignores the tombstone messages.
	 * For example this is the behaviour in case of RabbitMQ binder.
	 */
	@Bean
	@ConditionalOnMissingBean(CdcTombstoneConfiguration.TombstoneSupport.class)
	public CdcTombstoneConfiguration.TombstoneSupport nullTombstoneHelper() {
		return new CdcTombstoneConfiguration.TombstoneSupport() {
			@Override
			public boolean isKafkaPresent() {
				return false;
			}

			@Override
			public Object getTombstonePayload() {
				return null;
			}
		};
	}
}
