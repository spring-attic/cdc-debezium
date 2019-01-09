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
	private Map<String, String> config = new HashMap<>();

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
}
