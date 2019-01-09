/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.cdc.source;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.connect.source.SourceRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.app.cdc.common.core.CdcCommonConfiguration;
import org.springframework.cloud.stream.app.cdc.common.core.SpringEmbeddedEngine;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.reactive.StreamEmitter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeTypeUtils;

/**
 * CDC source that uses the Debezium Connectors to monitor and record all of the row-level changes in the databases.
 *
 * https://debezium.io/docs/connectors
 *
 * @author Christian Tzolov
 */
@EnableBinding(Source.class)
@EnableConfigurationProperties({ CdcSourceProperties.class })
@Import(CdcCommonConfiguration.class)
public class CdcSourceConfiguration {

	private static final Log logger = LogFactory.getLog(CdcSourceConfiguration.class);

	@Autowired
	private Consumer<FluxSink<Message<byte[]>>> engine;

	@Autowired
	private CdcSourceProperties properties;

	@StreamEmitter
	@Output(Source.OUTPUT)
	public Flux<Message<byte[]>> emit() {
		return Flux.create(engine);
	}

	@Bean
	public Consumer<FluxSink<Message<byte[]>>> engine(SpringEmbeddedEngine.Builder embeddedEngineBuilder,
			Function<SourceRecord, byte[]> valueSerializer, Function<SourceRecord, SourceRecord> recordFlattering) {

		return emitter -> {

			Consumer<byte[]> messageConsumer = cdcJsonPayload -> {

				logger.info("CDC Event -> " + new String(cdcJsonPayload));

				Message<byte[]> message = MessageBuilder
						.withPayload(cdcJsonPayload)
						.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE)
						.build();

				emitter.next(message);
			};

			SpringEmbeddedEngine engine = embeddedEngineBuilder
					.notifying(record -> messageConsumer.accept(recordFlattering.andThen(valueSerializer).apply(record)))
					.build();

			ExecutorService executor = Executors.newSingleThreadExecutor();
			executor.execute(engine);

			emitter.onDispose(() -> {
				engine.stop();
				executor.shutdown();
			});
		};
	}

}
