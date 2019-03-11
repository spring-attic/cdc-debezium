/*
 * Copyright 2019 the original author or authors.
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

package cdc.autoconfigure;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.connect.source.SourceRecord;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.stream.app.cdc.common.core.CdcCommonConfiguration;
import org.springframework.cloud.stream.app.cdc.common.core.EmbeddedEngine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * @author Christian Tzolov
 */
@Configuration
@Import({ CdcCommonConfiguration.class })
public class CdcAutoConfiguration {

	private static final Log logger = LogFactory.getLog(CdcAutoConfiguration.class);

	@Bean
	@ConditionalOnMissingBean
	public Consumer<SourceRecord> defaultSourceRecordConsumer() {
		return sourceRecord -> logger.info("[CDC Event]: " + sourceRecord.toString());
	}

	@Bean
	public EmbeddedEngineExecutorService embeddedEngine(EmbeddedEngine.Builder embeddedEngineBuilder,
			Consumer<SourceRecord> sourceRecordConsumer, Function<SourceRecord, SourceRecord> recordFlattering) {

		EmbeddedEngine embeddedEngine = embeddedEngineBuilder
				.notifying(sourceRecord -> sourceRecordConsumer.accept(recordFlattering.apply(sourceRecord)))
				.build();

		return new EmbeddedEngineExecutorService(embeddedEngine);
	}


	private static class EmbeddedEngineExecutorService implements Closeable {

		private EmbeddedEngine engine;
		private ExecutorService executor;

		private EmbeddedEngineExecutorService(EmbeddedEngine engine) {
			this.engine = engine;
			this.executor = Executors.newSingleThreadExecutor();
		}

		@PostConstruct
		public void start() {
			logger.info("Start Embedded Engine");
			this.executor.execute(this.engine);
		}

		@PreDestroy
		@Override
		public void close() {
			logger.info("Stop Embedded Engine");
			this.engine.stop();
			this.executor.shutdown();
		}
	}
}
