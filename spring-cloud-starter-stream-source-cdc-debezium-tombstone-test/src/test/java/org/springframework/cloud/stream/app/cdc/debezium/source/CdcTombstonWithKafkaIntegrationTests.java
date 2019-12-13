/*
 * Copyright 2018 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.messaging.Message;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.jdbc.JdbcTestUtils;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * When Spring-Kafka is present on the classpath and dropTombstones=false, Debezium generates extra (tombstone) message
 * with null payload. Binders don't allow null paylaods. To preserver the tumbsotne semantics with Spring-Kafka we need to
 * send KafkaNull payload.
 *
 * @author Christian Tzolov
 */
@RunWith(SpringRunner.class)
@SpringBootTest(
		webEnvironment = SpringBootTest.WebEnvironment.NONE,
		properties = {
				"spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration",
				"cdc.name=my-sql-connector",
				"cdc.schema=false",
				"cdc.config.database.history=io.debezium.relational.history.MemoryDatabaseHistory",
				"cdc.connector=mysql",
				"cdc.schema=false",
				"cdc.flattering.enabled=true",
				"cdc.config.database.user=debezium",
				"cdc.config.database.password=dbz",
				"cdc.config.database.hostname=localhost",
				"cdc.config.database.port=3306",
				"cdc.config.database.server.id=85744",
				"cdc.config.database.server.name=my-app-connector",
		})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public abstract class CdcTombstonWithKafkaIntegrationTests {

	@Autowired
	protected Source channels;

	@Autowired
	protected MessageCollector messageCollector;

	@TestPropertySource(properties = {
			"cdc.stream.header.key=true",
			"cdc.stream.header.offset=true",
			"cdc.flattering.dropTombstones=false" // E.g. generate tombstones events or record deletion
	})
	public static class CdcHandleDeletionWithTombstonesTests extends CdcTombstonWithKafkaIntegrationTests {

		@Test
		public void testOne() throws InterruptedException {

			JdbcTemplate jdbcTemplate = CdcTombstonWithKafkaIntegrationTests.jdbcTemplate(
					"com.mysql.cj.jdbc.Driver", "jdbc:mysql://localhost:3306/inventory", "root", "debezium");
			jdbcTemplate.update("insert into `customers`(`first_name`,`last_name`,`email`) VALUES('Test666', 'Test666', 'Test666@spring.org')");
			String newRecordId = jdbcTemplate.query("select * from `customers` where `first_name` = ?",
					(rs, rowNum) -> rs.getString("id"), "Test666").iterator().next();

			List<Message<?>> messages = CdcTombstonWithKafkaIntegrationTests.drain(messageCollector.forChannel(this.channels.output()));
			assertThat(messages.size(), is(53));

			JdbcTestUtils.deleteFromTableWhere(jdbcTemplate, "customers", "first_name = ?", "Test666");

			Message<?> received = messageCollector.forChannel(this.channels.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);

			// Because tombstones are not dropped second tombstones message is send;
			received = messageCollector.forChannel(this.channels.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat("Tombstones event should have KafkaNull payload",
					received.getPayload().getClass().getCanonicalName(), is("org.springframework.kafka.support.KafkaNull"));

			String key = (String) received.getHeaders().get("cdc.key");
			assertThat("Tombstones event should carry the deleted record id in the cdc.key header",
					key, is("{\"id\":" + newRecordId + "}"));

			received = messageCollector.forChannel(this.channels.output()).poll(1, TimeUnit.SECONDS);
			assertNull(received);
		}
	}

	@TestPropertySource(properties = {
			"cdc.stream.header.key=true",
			"cdc.stream.header.offset=true",
			"cdc.flattering.dropTombstones=true" // drop the Tombstones events on record deletion - Default behavior
	})
	public static class CdcHandleDeletionWithDropTombstonesTests extends CdcTombstonWithKafkaIntegrationTests {

		@Test
		public void testOne() throws InterruptedException {

			JdbcTemplate jdbcTemplate = CdcTombstonWithKafkaIntegrationTests.jdbcTemplate(
					"com.mysql.cj.jdbc.Driver", "jdbc:mysql://localhost:3306/inventory", "root", "debezium");
			jdbcTemplate.update("insert into `customers`(`first_name`,`last_name`,`email`) VALUES('Test666', 'Test666', 'Test666@spring.org')");

			List<Message<?>> messages = CdcTombstonWithKafkaIntegrationTests.drain(messageCollector.forChannel(this.channels.output()));
			assertThat(messages.size(), is(53));

			JdbcTestUtils.deleteFromTableWhere(jdbcTemplate, "customers", "first_name = ?", "Test666");

			Message<?> received = messageCollector.forChannel(this.channels.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);

			received = messageCollector.forChannel(this.channels.output()).poll(1, TimeUnit.SECONDS);
			assertNull(received);
		}
	}

	@SpringBootConfiguration
	@EnableAutoConfiguration
	@Import(CdcDebeziumSourceConfiguration.class)
	public static class TestCdcSourceApplication {

	}

	private static JdbcTemplate jdbcTemplate(String jdbcDriver, String jdbcUrl, String user, String password) {

		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName(jdbcDriver);
		dataSource.setUrl(jdbcUrl);
		dataSource.setUsername(user);
		dataSource.setPassword(password);

		return new JdbcTemplate(dataSource);
	}

	private static List<Message<?>> drain(BlockingQueue<Message<?>> queue) throws InterruptedException {
		List<Message<?>> list = new ArrayList<>();
		Message<?> received;
		do {
			received = queue.poll(10, TimeUnit.SECONDS);
			if (received != null) {
				list.add(received);
			}
		} while (received != null);
		return list;
	}
}
