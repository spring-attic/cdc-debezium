/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.cdc.debezium.source;

import java.util.List;

import org.junit.Test;

import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.messaging.Message;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.springframework.cloud.stream.app.cdc.debezium.source.CdcTestUtils.drain;

/**
 * @author Christian Tzolov
 */
public class CdcSourceDatabasesIntegrationTest {

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
			.withUserConfiguration(TestCdcSourceApplication.class)
			.withPropertyValues(
					"cdc.name=my-sql-connector",
					"cdc.flattering.dropTombstones=false",
					"cdc.schema=false",
					"cdc.flattering.enabled=true",
					"cdc.stream.header.offset=true",

					"cdc.config.database.server.id=85744",
					"cdc.config.database.server.name=my-app-connector",
					"cdc.config.database.history=io.debezium.relational.history.MemoryDatabaseHistory",
					"spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration");

	@Test
	public void mysql() {
		contextRunner
				.withPropertyValues(
						"cdc.connector=mysql",
						"cdc.config.database.user=debezium",
						"cdc.config.database.password=dbz",
						"cdc.config.database.hostname=localhost",
						"cdc.config.database.port=3306"
				)
				.run((context -> {
					Source channels = context.getBean(Source.class);
					MessageCollector messageCollector = context.getBean(MessageCollector.class);

					List<Message<?>> messages = drain(messageCollector.forChannel(channels.output()));
					assertThat(messages.size(), is(52));
				}));
	}

	@Test
	public void sqlServer() {
		contextRunner
				.withPropertyValues(
						"cdc.connector=sqlserver",
						//"cdc.config.database.user=Standard",
						"cdc.config.database.user=sa",
						"cdc.config.database.password=Password!",
						"cdc.config.database.dbname=testDB",

						"cdc.config.database.hostname=localhost",
						"cdc.config.database.port=1433"
				)
				.run((context -> {
					Source channels = context.getBean(Source.class);
					MessageCollector messageCollector = context.getBean(MessageCollector.class);

					List<Message<?>> messages = drain(messageCollector.forChannel(channels.output()));
					assertThat(messages.size(), is(26));
				}));
	}

	@Test
	public void postgres() {
		contextRunner
				.withPropertyValues(
						"cdc.connector=postgres",
						"cdc.config.database.user=postgres",
						"cdc.config.database.password=postgres",
						"cdc.config.database.dbname=postgres",
						"cdc.config.database.hostname=localhost",
						"cdc.config.database.port=5432"
				)
				.run((context -> {
					Source channels = context.getBean(Source.class);
					MessageCollector messageCollector = context.getBean(MessageCollector.class);

					List<Message<?>> messages = drain(messageCollector.forChannel(channels.output()));
					assertThat(messages.size(), is(5786));
				}));
	}

	//@Test
	public void mongodb() {
		contextRunner
				.withPropertyValues(
						"cdc.connector=mongodb",
						"cdc.config.tasks.max=1",
						"cdc.config.mongodb.hosts=rs0/localhost:27017",
						"cdc.config.mongodb.name=dbserver1",
						"cdc.config.mongodb.user=debezium",
						"cdc.config.mongodb.password=dbz",
						"cdc.config.database.whitelist=inventory"
				)
				.run((context -> {
					Source channels = context.getBean(Source.class);
					MessageCollector messageCollector = context.getBean(MessageCollector.class);

					List<Message<?>> messages = drain(messageCollector.forChannel(channels.output()));
					assertThat(messages.size(), is(666));
				}));
	}
}
