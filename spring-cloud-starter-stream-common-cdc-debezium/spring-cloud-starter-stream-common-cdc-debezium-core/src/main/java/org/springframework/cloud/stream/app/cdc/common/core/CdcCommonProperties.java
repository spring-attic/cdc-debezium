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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.sqlserver.SqlServerConnector;
import io.debezium.embedded.spi.OffsetCommitPolicy;
import io.debezium.relational.history.MemoryDatabaseHistory;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.app.cdc.common.core.store.MetadataStoreOffsetBackingStore;
import org.springframework.validation.annotation.Validated;

/**
 *
 * @author Christian Tzolov
 */
@ConfigurationProperties("cdc")
@Validated
public class CdcCommonProperties {

	/**
	 * Unique name for this sourceConnector instance.
	 */
	@NotEmpty
	private String name;

	/**
	 * Shortcut for the cdc.config.connector.class property. Either of those can be used as long as they do not
	 * contradict with each other.
	 */
	@NotNull
	private ConnectorType connector = null;

	private Offset offset = new Offset();

	public static class Offset {

		/**
		 * Interval at which to try committing offsets. The default is 1 minute.
		 */
		private Duration flushInterval = Duration.ofMillis(60000);

		/**
		 * Maximum number of milliseconds to wait for records to flush and partition offset data to be committed to
		 * offset storage before cancelling the process and restoring the offset data to be committed in a future attempt.
		 */
		private Duration commitTimeout = Duration.ofMillis(5000);

		/**
		 * Offset storage commit policy.
		 */
		private OffsetPolicy policy = OffsetPolicy.periodic;

		/**
		 * When a Kafka Connect connector runs, it reads information from the source and periodically records "offsets"
		 * that define how much of that information it has processed. Should the connector be restarted, it will use the
		 * last recorded offset to know where in the source information it should resume reading.
		 */
		private OffsetStorageType storage = OffsetStorageType.metadata;

		public enum OffsetPolicy {
			periodic(OffsetCommitPolicy.PeriodicCommitOffsetPolicy.class.getName()),
			always(OffsetCommitPolicy.AlwaysCommitOffsetPolicy.class.getName());

			public final String policyClass;

			OffsetPolicy(String policyClassName) {
				this.policyClass = policyClassName;
			}
		}

		public Duration getFlushInterval() {
			return flushInterval;
		}

		public void setFlushInterval(Duration flushInterval) {
			this.flushInterval = flushInterval;
		}

		public Duration getCommitTimeout() {
			return commitTimeout;
		}

		public void setCommitTimeout(Duration commitTimeout) {
			this.commitTimeout = commitTimeout;
		}

		public OffsetPolicy getPolicy() {
			return policy;
		}

		public void setPolicy(OffsetPolicy policy) {
			this.policy = policy;
		}

		public OffsetStorageType getStorage() {
			return storage;
		}

		public void setStorage(OffsetStorageType storage) {
			this.storage = storage;
		}

	}

	/**
	 * If set then the value's schema is included as part of the the outbound message.
	 */
	private boolean schema = false;

	/**
	 * Event Flattering (https://debezium.io/docs/configuration/event-flattening)
	 */
	private Flattering flattering = new Flattering();

	/**
	 * Spring pass-trough wrapper for the debezium configuration properties. All properties with 'cdc.config' prefix
	 * are converted into Debezium io.debezium.config.Configuration and the prefix is dropped.
	 */
	private Map<String, String> config = defaultConfig();

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Offset getOffset() {
		return offset;
	}

	public enum OffsetStorageType {
		memory(MemoryOffsetBackingStore.class.getName()),
		file(FileOffsetBackingStore.class.getName()),
		kafka(KafkaOffsetBackingStore.class.getName()),
		metadata(MetadataStoreOffsetBackingStore.class.getName());

		public final String offsetStorageClass;

		OffsetStorageType(String type) {
			this.offsetStorageClass = type;
		}
	}

	public enum ConnectorType {
		mysql(MySqlConnector.class.getName()),
		postgres(PostgresConnector.class.getName()),
		mongodb(CdcConnectorConfiguration.MongodbSourceConnector.class.getName()),
		oracle(OracleConnector.class.getName()),
		sqlserver(SqlServerConnector.class.getName());

		public final String connectorClass;

		ConnectorType(String type) {
			this.connectorClass = type;
		}
	}

	public enum DeleteHandlingMode {none, drop, rewrite}

	/**
	 * https://debezium.io/docs/configuration/event-flattening
	 */
	public static class Flattering {

		/**
		 * Enable flattering the source record events (https://debezium.io/docs/configuration/event-flattening).
		 */
		private boolean enabled = true;

		/**
		 * Debezium by default generates a tombstone record to enable Kafka compaction after a delete record was generated.
		 * This record is usually filtered out to avoid duplicates as a delete record is converted to a tombstone record, too.
		 */
		private boolean dropTombstones = true;

		/**
		 * How to handle delete records. Options are: (1) none - records are passed, (2) drop - records are removed and
		 * (3) rewrite - adds '__deleted' field to the records.
		 */
		private DeleteHandlingMode deleteHandlingMode = DeleteHandlingMode.none;

		public boolean isEnabled() {
			return enabled;
		}

		public void setEnabled(boolean enabled) {
			this.enabled = enabled;
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

	public boolean isSchema() {
		return schema;
	}

	public void setSchema(boolean schema) {
		this.schema = schema;
	}

	private Map<String, String> defaultConfig() {
		Map<String, String> defaultConfig = new HashMap<>();
		defaultConfig.put("database.history", MemoryDatabaseHistory.class.getName());
		//defaultConfig.put("offset.flush.interval.ms", "60000");
		return defaultConfig;
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
