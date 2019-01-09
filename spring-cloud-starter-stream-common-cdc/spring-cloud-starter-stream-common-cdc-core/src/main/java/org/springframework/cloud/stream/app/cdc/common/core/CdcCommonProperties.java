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

import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 *
 * @author Christian Tzolov
 */
@ConfigurationProperties("cdc")
@Validated
public class CdcCommonProperties {

	/**
	 * If true the value's schema is included in the outbound message.
	 */
	private boolean includeSchema = false;

	/**
	 * Event Flattering (https://debezium.io/docs/configuration/event-flattening)
	 */
	private Flattering flattering = new Flattering();

	/**
	 * Spring pass-trough wrapper for the debezium configuration properties. All properties with 'cdc.config' prefix
	 * are converted into Debezium io.debezium.config.Configuration and the prefix is dropped.
	 */
	private Map<String, String> config = defaultConfig();

	public enum OffsetStorageType {
		memory("org.apache.kafka.connect.storage.MemoryOffsetBackingStore"),
		file("org.apache.kafka.connect.storage.FileOffsetBackingStore"),
		kafka("org.apache.kafka.connect.storage.KafkaOffsetBackingStore"),
		metadata("org.springframework.cloud.stream.app.cdc.common.core.store.MetadataStoreOffsetBackingStore");

		public final String offsetStorageClass;

		OffsetStorageType(String type) {
			this.offsetStorageClass = type;
		}
	}

	/**
	 * When a Kafka Connect connector runs, it reads information from the source and periodically records "offsets"
	 * that define how much of that information it has processed. Should the connector be restarted, it will use the
	 * last recorded offset to know where in the source information it should resume reading.
	 */
	private OffsetStorageType offsetStorage = OffsetStorageType.metadata;


	public enum ConnectorType {
		mysql("io.debezium.connector.mysql.MySqlConnector"),
		postgres("io.debezium.connector.postgresql.PostgresConnector"),
		mongodb("io.debezium.connector.mongodb.MongodbSourceConnector"),
		oracle("io.debezium.connector.oracle.OracleConnector"),
		sqlserver("io.debezium.connector.sqlserver.SqlServerConnector");

		public final String connectorClass;

		ConnectorType(String type) {
			this.connectorClass = type;
		}
	}

	/**
	 * Shortcut for the cdc.config.connector.class property. Either of those can be used as long as they do not
	 * contradict with each other.
	 */
	@NotNull
	private ConnectorType connector = null;

	/**
	 * https://debezium.io/docs/configuration/event-flattening
	 */
	public static class Flattering {

		public enum DeleteHandlingMode {none, drop, rewrite}

		/**
		 * Enable flattering the source record events (https://debezium.io/docs/configuration/event-flattening).
		 */
		private boolean enabled = false;

		/**
		 * Debezium by default generates a tombstone record to enable Kafka compaction after a delete record was generated.
		 * This record is usually filtered out to avoid duplicates as a delete record is converted to a tombstone record, too.
		 */
		private boolean dropTombstones = true;

		/**
		 * Drop delete records converted to tombstones records if a processing connector cannot process them or a
		 * compaction is undesirable.
		 */
		private boolean dropDeletes = false;


		/**
		 * How to handle delete records. Options are: (1) none - records are passed, (2) drop - records are removed and
		 * (3) rewrite - adds '__deleted' field to the records.
		 */
		private DeleteHandlingMode deleteHandlingMode = DeleteHandlingMode.drop;

		public boolean isEnabled() {
			return enabled;
		}

		public void setEnabled(boolean enabled) {
			this.enabled = enabled;
		}

		public boolean isDropDeletes() {
			return dropDeletes;
		}

		public void setDropDeletes(boolean dropDeletes) {
			this.dropDeletes = dropDeletes;
		}

		public boolean isDropTombstones() {
			return dropTombstones;
		}

		public void setDropTombstones(boolean dropTombstones) {
			this.dropTombstones = dropTombstones;
		}

		public DeleteHandlingMode getDeleteHandlingMode() {
			return deleteHandlingMode;
		}

		public void setDeleteHandlingMode(DeleteHandlingMode deleteHandlingMode) {
			this.deleteHandlingMode = deleteHandlingMode;
		}
	}

	public Flattering getFlattering() {
		return flattering;
	}

	public Map<String, String> getConfig() {
		return config;
	}

	public boolean isIncludeSchema() {
		return includeSchema;
	}

	public void setIncludeSchema(boolean includeSchema) {
		this.includeSchema = includeSchema;
	}

	private Map<String, String> defaultConfig() {
		Map<String, String> defaultConfig = new HashMap<>();
		defaultConfig.put("database.history", "io.debezium.relational.history.MemoryDatabaseHistory");
		defaultConfig.put("offset.flush.interval.ms", "60000");
		return defaultConfig;
	}

	public OffsetStorageType getOffsetStorage() {
		return offsetStorage;
	}

	public void setOffsetStorage(OffsetStorageType offsetStorage) {
		this.offsetStorage = offsetStorage;
	}

	public ConnectorType getConnector() {
		return connector;
	}

	public void setConnector(ConnectorType connector) {
		this.connector = connector;
	}

	@AssertTrue
	public boolean connectorIsSet() {
		return this.getConnector() != null || this.getConfig().containsKey("connector.class");
	}
}
