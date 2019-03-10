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

import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.sqlserver.SqlServerConnector;
import org.apache.kafka.connect.source.SourceConnector;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Christian Tzolov
 */
@Configuration
public class CdcConnectorConfiguration {

	@ConditionalOnClass(MySqlConnector.class)
	@ConditionalOnExpression("'${cdc.config.connector.class}'.equalsIgnoreCase('io.debezium.connector.mysql.MySqlConnector') " +
			"or '${cdc.connector}'.equalsIgnoreCase('mysql')")
	public static class MySqlSourceConnector {
		@Bean
		@ConditionalOnMissingBean
		public SourceConnector mysqlConnector() {
			return new MySqlConnector();
		}
	}

	@ConditionalOnClass(PostgresConnector.class)
	@ConditionalOnExpression("'${cdc.config.connector.class}'.equalsIgnoreCase('io.debezium.connector.postgresql.PostgresConnector') " +
			"or '${cdc.connector}'.equalsIgnoreCase('postgres')")
	public static class PostgresSourceConnector {
		@Bean
		@ConditionalOnMissingBean
		public SourceConnector postgresConnector() {
			return new PostgresConnector();
		}
	}

	@ConditionalOnClass(MongoDbConnector.class)
	@ConditionalOnExpression("'${cdc.config.connector.class}'.equalsIgnoreCase('io.debezium.connector.mongodb.MongodbSourceConnector') " +
			"or '${cdc.connector}'.equalsIgnoreCase('mongodb')")
	public static class MongodbSourceConnector {
		@Bean
		@ConditionalOnMissingBean
		public SourceConnector mongodbConnector() {
			return new MongoDbConnector();
		}
	}

	@ConditionalOnClass(OracleConnector.class)
	@ConditionalOnExpression("'${cdc.config.connector.class}'.equalsIgnoreCase('io.debezium.connector.oracle.OracleConnector') " +
			"or '${cdc.connector}'.equalsIgnoreCase('oracle')")
	public static class OracleSourceConnector {
		@Bean
		@ConditionalOnMissingBean
		public SourceConnector oracleConnector() {
			return new OracleConnector();
		}
	}

	@ConditionalOnClass(SqlServerConnector.class)
	@ConditionalOnExpression("'${cdc.config.connector.class}'.equalsIgnoreCase('io.debezium.connector.sqlserver.SqlServerConnector') " +
			"or '${cdc.connector}'.equalsIgnoreCase('sqlserver')")
	public static class SqlServerSourceConnector {
		@Bean
		@ConditionalOnMissingBean
		public SourceConnector sqlServerConnector() {
			return new SqlServerConnector();
		}
	}
}
