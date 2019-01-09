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

import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.stream.app.cdc.common.core.store.MetadataStoreOffsetBackingStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.metadata.MetadataStore;

/**
 * @author Christian Tzolov
 */
@Configuration
public class CdcOffsetBackingStoreConfiguration {

	public static final String CDC_CONFIG_OFFSET_STORAGE = "cdc.config.offset.storage";

	@Bean
	@ConditionalOnMissingBean
	@ConditionalOnProperty(name = CDC_CONFIG_OFFSET_STORAGE,
			havingValue = "MetadataStoreOffsetBackingStore",
			matchIfMissing = true)
	public OffsetBackingStore metadataStoreOffsetBackingStore(MetadataStore metadataStore) {
		return new MetadataStoreOffsetBackingStore(metadataStore);
	}

	@Bean
	@ConditionalOnMissingBean
	@ConditionalOnProperty(name = CDC_CONFIG_OFFSET_STORAGE,
			havingValue = "org.apache.kafka.connect.storage.FileOffsetBackingStore")
	public OffsetBackingStore fileOffsetBackingStore() {
		return new FileOffsetBackingStore();
	}

	@Bean
	@ConditionalOnMissingBean
	@ConditionalOnProperty(name = CDC_CONFIG_OFFSET_STORAGE,
			havingValue = "org.apache.kafka.connect.storage.KafkaOffsetBackingStore")
	public OffsetBackingStore kafkaOffsetBackingStore() {
		return new KafkaOffsetBackingStore();
	}

	@Bean
	@ConditionalOnMissingBean
	@ConditionalOnProperty(name = CDC_CONFIG_OFFSET_STORAGE,
			havingValue = "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
	public OffsetBackingStore memoryOffsetBackingStore() {
		return new MemoryOffsetBackingStore();
	}
}
