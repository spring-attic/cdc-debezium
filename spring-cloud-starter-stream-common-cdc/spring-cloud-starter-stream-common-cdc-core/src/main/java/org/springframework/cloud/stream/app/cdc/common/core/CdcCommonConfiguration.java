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

package org.springframework.cloud.stream.app.cdc.common.core;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import io.debezium.transforms.UnwrapFromEnvelope;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetBackingStore;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * @author Christian Tzolov
 */
@Configuration
@EnableConfigurationProperties({ CdcCommonProperties.class })
@Import({ CdcConnectorConfiguration.class, CdcOffsetBackingStoreConfiguration.class })
public class CdcCommonConfiguration {

	private static final Log logger = LogFactory.getLog(CdcCommonConfiguration.class);

	@Bean
	public io.debezium.config.Configuration configuration(CdcCommonProperties properties) {
		Map<String, String> configMap = properties.getConfig();
		return io.debezium.config.Configuration.from(configMap);
	}

	@Bean
	public Function<SourceRecord, SourceRecord> recordFlattering(CdcCommonProperties properties) {
		return sourceRecord -> properties.getFlattering().isEnabled() ?
				(SourceRecord) unwrapFromEnvelope(properties).apply(sourceRecord) : sourceRecord;
	}

	private UnwrapFromEnvelope unwrapFromEnvelope(CdcCommonProperties properties) {
		UnwrapFromEnvelope unwrapFromEnvelope = new UnwrapFromEnvelope();
		Map<String, Object> config = unwrapFromEnvelope.config().defaultValues();
		config.put("drop.tombstones", properties.getFlattering().isDropTombstones());
		config.put("drop.deletes", properties.getFlattering().isDropDeletes());
		config.put("delete.handling.mode", properties.getFlattering().getDeleteHandlingMode().name());
		unwrapFromEnvelope.configure(config);

		return unwrapFromEnvelope;
	}

	@Bean
	@ConditionalOnMissingBean
	public JsonConverter jsonConverter(CdcCommonProperties properties) {
		JsonConverter jsonConverter = new JsonConverter();
		jsonConverter.configure(Collections.singletonMap("schemas.enable", properties.isIncludeSchema()), false);
		return jsonConverter;
	}

	@Bean
	public Function<SourceRecord, byte[]> valueSerializer(Converter valueConverter) {
		return sourceRecord -> valueConverter.fromConnectData(
				sourceRecord.topic(), sourceRecord.valueSchema(), sourceRecord.value());
	}

	@Bean
	public SpringEmbeddedEngine.Builder embeddedEngineBuilder(CdcCommonProperties properties,
			SourceConnector sourceConnector, OffsetBackingStore offsetBackingStore) {

		return SpringEmbeddedEngine.create()
				.using(io.debezium.config.Configuration.from(properties.getConfig()))
				.sourceConnector(sourceConnector)
				.offsetBackingStore(offsetBackingStore);
	}

}
