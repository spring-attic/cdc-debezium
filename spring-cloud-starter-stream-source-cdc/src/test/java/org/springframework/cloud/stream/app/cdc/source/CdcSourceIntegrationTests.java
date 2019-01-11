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

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.Message;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Christian Tzolov
 */
@RunWith(SpringRunner.class)
@SpringBootTest(
		webEnvironment = SpringBootTest.WebEnvironment.NONE,
		properties = {
				"cdc.name=my-sql-connector",
				"cdc.schema=false",
				"cdc.config.database.history=io.debezium.relational.history.MemoryDatabaseHistory",
		})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public abstract class CdcSourceIntegrationTests {

	@Autowired
	protected Source channels;

	@Autowired
	protected MessageCollector messageCollector;

	@TestPropertySource(properties = {
			"cdc.connector=mysql",
			"cdc.schema=false",
			"cdc.flattering.enabled=true",
			//"cdc.offset.storage=memory",

			"cdc.config.database.user=debezium",
			"cdc.config.database.password=dbz",
			"cdc.config.database.hostname=localhost",
			"cdc.config.database.port=3306",

			"cdc.config.database.server.id=85744",
			"cdc.config.database.server.name=my-app-connector",

			"cdc.stream.header.key=true",
			"cdc.stream.header.offset=true",

	})
	public static class CdcMysqlTests extends CdcSourceIntegrationTests {

		@Test
		public void testOne() throws InterruptedException {

			Message<?> received = messageCollector.forChannel(this.channels.output()).poll(10, TimeUnit.SECONDS);
			Assert.assertNotNull(received);

			do {
				received = messageCollector.forChannel(this.channels.output()).poll(10, TimeUnit.SECONDS);
				if (received != null) {
					System.out.println("Headers: " + received.getHeaders());
					System.out.println("Payload: " + received.getPayload());
				}
			} while (received != null);

		}
	}

	@TestPropertySource(properties = {
			// "cdc.config.connector.class=io.debezium.connector.postgresql.PostgresConnector",
			"cdc.connector=postgres",

			"cdc.config.database.user=postgres",
			"cdc.config.database.password=postgres",

			"cdc.config.database.dbname=postgres",
			"cdc.config.database.hostname=localhost",
			"cdc.config.database.port=5432",

			"cdc.config.database.server.name=my-app-connector",
	})
	public static class CdcPostgresTests extends CdcSourceIntegrationTests {

		@Test
		public void testOne() throws InterruptedException {

			Message<?> received = messageCollector.forChannel(this.channels.output()).poll(10, TimeUnit.SECONDS);
			Assert.assertNotNull(received);

			do {
				received = messageCollector.forChannel(this.channels.output()).poll(10, TimeUnit.SECONDS);
				if (received != null) {
					System.out.println("Headers: " + received.getHeaders());
					System.out.println("Payload: " + received.getPayload());
				}
			} while (received != null);

		}
	}

	@TestPropertySource(properties = {
			//"cdc.config.connector.class=io.debezium.connector.sqlserver.SqlServerConnector",
			"cdc.connector=sqlserver",
			"cdc.config.database.user=Standard",
			"cdc.config.database.password=Password!",

			"cdc.config.database.hostname=localhost",
			"cdc.config.database.port=1433"
	})
	public static class CdcSqlServerTests extends CdcSourceIntegrationTests {

		@Ignore(" Currently SQL Server Connector is only available at 0.9.0.Beta2")
		@Test
		public void testOne() throws InterruptedException {

			Message<?> received = messageCollector.forChannel(this.channels.output()).poll(10, TimeUnit.SECONDS);
			Assert.assertNotNull(received);

			do {
				received = messageCollector.forChannel(this.channels.output()).poll(10, TimeUnit.SECONDS);
				if (received != null) {
					System.out.println("Headers: " + received.getHeaders());
					System.out.println("Payload: " + received.getPayload());
				}
			} while (received != null);

		}
	}

	@TestPropertySource(properties = {
			"cdc.connector=mongodb",

			"cdc.config.tasks.max=1",
			"cdc.config.mongodb.hosts=rs0/localhost:27017",
			"cdc.config.mongodb.name=dbserver1",
			"cdc.config.mongodb.user=debezium",
			"cdc.config.mongodb.password=dbz",
			"cdc.config.database.whitelist=inventory",
	})
	public static class CdcSqlMongoDbTests extends CdcSourceIntegrationTests {

		@Test
		public void testOne() throws InterruptedException {

			Message<?> received = messageCollector.forChannel(this.channels.output()).poll(10, TimeUnit.SECONDS);
			Assert.assertNotNull(received);

			do {
				received = messageCollector.forChannel(this.channels.output()).poll(10, TimeUnit.SECONDS);
				if (received != null) {
					System.out.println("Headers: " + received.getHeaders());
					System.out.println("Payload: " + received.getPayload());
				}
			} while (received != null);

		}
	}

	@SpringBootConfiguration
	@EnableAutoConfiguration
	@Import(CdcSourceConfiguration.class)
	public static class TestCdcSourceApplication {

	}
}
