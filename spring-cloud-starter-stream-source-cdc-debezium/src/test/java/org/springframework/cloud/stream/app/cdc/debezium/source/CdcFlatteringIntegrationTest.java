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

import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.context.runner.ContextConsumer;
import org.springframework.cloud.stream.app.cdc.common.core.CdcCommonProperties;
import org.springframework.cloud.stream.app.cdc.common.core.CdcTombstoneConfiguration;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;
import org.springframework.test.jdbc.JdbcTestUtils;

import static net.javacrumbs.jsonunit.JsonAssert.assertJsonEquals;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.springframework.cloud.stream.app.cdc.common.core.CdcTombstoneConfiguration.ORG_SPRINGFRAMEWORK_KAFKA_SUPPORT_KAFKA_NULL;
import static org.springframework.cloud.stream.app.cdc.debezium.source.CdcTestUtils.drain;
import static org.springframework.cloud.stream.app.cdc.debezium.source.CdcTestUtils.jdbcTemplate;
import static org.springframework.cloud.stream.app.cdc.debezium.source.CdcTestUtils.resourceToString;

/**
 * @author Christian Tzolov
 */
public class CdcFlatteringIntegrationTest {

	private final JdbcTemplate jdbcTemplate = jdbcTemplate(
			"com.mysql.cj.jdbc.Driver",
			"jdbc:mysql://localhost:3306/inventory",
			"root", "debezium");

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
			.withUserConfiguration(TestCdcSourceApplication.class)
			.withPropertyValues(
					"cdc.name=my-sql-connector",
					"cdc.schema=false",

					"cdc.stream.header.offset=false",

					"cdc.connector=mysql",
					"cdc.config.database.user=debezium",
					"cdc.config.database.password=dbz",
					"cdc.config.database.hostname=localhost",
					"cdc.config.database.port=3306",
					"cdc.config.database.server.id=85744",
					"cdc.config.database.server.name=my-app-connector",
					"cdc.config.database.history=io.debezium.relational.history.MemoryDatabaseHistory");

	@Test
	public void noFlatteredResponseNoKafka() {
		contextRunner.withPropertyValues("cdc.flattering.enabled=false")
				.withClassLoader(new FilteredClassLoader(KafkaNull.class)) // Remove Kafka from the classpath
				.run(noFlatteringTest);
	}

	@Test
	public void noFlatteredResponseWithKafka() {
		contextRunner.withPropertyValues("cdc.flattering.enabled=false")
				.run(noFlatteringTest);
	}

	final ContextConsumer<? super AssertableApplicationContext> noFlatteringTest = context -> {
		Source channels = context.getBean(Source.class);
		MessageCollector messageCollector = context.getBean(MessageCollector.class);
		CdcTombstoneConfiguration.TombstoneSupport tombstoneSupport =
				context.getBean(CdcTombstoneConfiguration.TombstoneSupport.class);

		List<Message<?>> messages = drain(messageCollector.forChannel(channels.output()));
		assertThat(messages.size(), is(52));

		assertJsonEquals(resourceToString(
				"classpath:/json/mysql_ddl_drop_inventory_address_table.json"),
				messages.get(1).getPayload().toString());
		assertEquals("my-app-connector",
				messages.get(1).getHeaders().get("cdc_topic"));
		assertJsonEquals("{\"databaseName\":\"inventory\"}",
				messages.get(1).getHeaders().get("cdc_key"));

		assertJsonEquals(resourceToString("classpath:/json/mysql_insert_inventory_products_106.json"),
				messages.get(39).getPayload().toString());
		assertEquals("my-app-connector.inventory.products", messages.get(39).getHeaders().get("cdc_topic"));
		assertJsonEquals("{\"id\":106}", messages.get(39).getHeaders().get("cdc_key"));

		jdbcTemplate.update(
				"insert into `customers`(`first_name`,`last_name`,`email`) VALUES('Test666', 'Test666', 'Test666@spring.org')");
		String newRecordId = jdbcTemplate.query("select * from `customers` where `first_name` = ?",
				(rs, rowNum) -> rs.getString("id"), "Test666").iterator().next();
		jdbcTemplate.update("UPDATE `customers` SET `last_name`='Test999' WHERE first_name = 'Test666'");
		JdbcTestUtils.deleteFromTableWhere(jdbcTemplate, "customers", "first_name = ?", "Test666");

		messages = drain(messageCollector.forChannel(channels.output()));

		assertThat(messages.size(), is(tombstoneSupport.isKafkaPresent() ? 4 : 3));

		assertJsonEquals(resourceToString("classpath:/json/mysql_update_inventory_customers.json"),
				messages.get(1).getPayload().toString());
		assertEquals("my-app-connector.inventory.customers", messages.get(1).getHeaders().get("cdc_topic"));
		assertJsonEquals("{\"id\":" + newRecordId + "}", messages.get(1).getHeaders().get("cdc_key"));

		assertJsonEquals(resourceToString("classpath:/json/mysql_delete_inventory_customers.json"),
				messages.get(2).getPayload().toString());
		assertEquals("my-app-connector.inventory.customers", messages.get(1).getHeaders().get("cdc_topic"));
		assertJsonEquals("{\"id\":" + newRecordId + "}", messages.get(1).getHeaders().get("cdc_key"));

		if (tombstoneSupport.isKafkaPresent()) {
			assertThat("Tombstones event should have KafkaNull payload",
					messages.get(3).getPayload().getClass().getCanonicalName(),
					is(ORG_SPRINGFRAMEWORK_KAFKA_SUPPORT_KAFKA_NULL));
			assertEquals("my-app-connector.inventory.customers", messages.get(3).getHeaders().get("cdc_topic"));
			assertJsonEquals("{\"id\":" + newRecordId + "}", messages.get(3).getHeaders().get("cdc_key"));
		}
	};

	@Test
	public void flatteredResponseNoKafka() {
		contextRunner.withPropertyValues(
				"cdc.flattering.enabled=true",
				"cdc.flattering.deleteHandlingMode=none",
				"cdc.flattering.dropTombstones=false",
				"cdc.flattering.operationHeader=true",
				"cdc.flattering.addSourceFields=name,db")
				.withClassLoader(new FilteredClassLoader(KafkaNull.class)) // Remove Kafka from the classpath
				.run(flatteringTest);
	}

	@Test
	public void flatteredResponseWithKafka() {
		contextRunner.withPropertyValues(
				"cdc.flattering.enabled=true",
				"cdc.flattering.deleteHandlingMode=none",
				"cdc.flattering.dropTombstones=false",
				"cdc.flattering.operationHeader=true",
				"cdc.flattering.addSourceFields=name,db")
				.run(flatteringTest);
	}

	@Test
	public void flatteredResponseWithKafkaDropTombstone() {
		contextRunner.withPropertyValues(
				"cdc.flattering.enabled=true",
				"cdc.flattering.deleteHandlingMode=none",
				"cdc.flattering.dropTombstones=true",
				"cdc.flattering.operationHeader=true",
				"cdc.flattering.addSourceFields=name,db")
				.run(flatteringTest);
	}

	final ContextConsumer<? super AssertableApplicationContext> flatteringTest = context -> {
		Source channels = context.getBean(Source.class);
		MessageCollector messageCollector = context.getBean(MessageCollector.class);
		CdcTombstoneConfiguration.TombstoneSupport tombstoneSupport =
				context.getBean(CdcTombstoneConfiguration.TombstoneSupport.class);
		CdcCommonProperties.Flattering flatteringProps = context.getBean(CdcCommonProperties.class).getFlattering();
		List<Message<?>> messages = drain(messageCollector.forChannel(channels.output()));
		assertThat(messages.size(), is(52));

		assertJsonEquals(resourceToString(
				"classpath:/json/mysql_ddl_drop_inventory_address_table.json"),
				messages.get(1).getPayload().toString());
		assertEquals("my-app-connector",
				messages.get(1).getHeaders().get("cdc_topic"));
		assertJsonEquals("{\"databaseName\":\"inventory\"}",
				messages.get(1).getHeaders().get("cdc_key"));

		if (flatteringProps.isEnabled()) {
			assertJsonEquals(resourceToString("classpath:/json/mysql_flattered_insert_inventory_products_106.json"),
					messages.get(39).getPayload().toString());
		}
		else {
			assertJsonEquals(resourceToString("classpath:/json/mysql_insert_inventory_products_106.json"),
					messages.get(39).getPayload().toString());
		}
		assertEquals("my-app-connector.inventory.products", messages.get(39).getHeaders().get("cdc_topic"));
		assertJsonEquals("{\"id\":106}", messages.get(39).getHeaders().get("cdc_key"));

		if (flatteringProps.isEnabled() && flatteringProps.isOperationHeader()) {
			assertEquals("c", messages.get(39).getHeaders().get("cdc_operation"));
		}

		jdbcTemplate.update(
				"insert into `customers`(`first_name`,`last_name`,`email`) VALUES('Test666', 'Test666', 'Test666@spring.org')");
		String newRecordId = jdbcTemplate.query("select * from `customers` where `first_name` = ?",
				(rs, rowNum) -> rs.getString("id"), "Test666").iterator().next();
		jdbcTemplate.update("UPDATE `customers` SET `last_name`='Test999' WHERE first_name = 'Test666'");
		JdbcTestUtils.deleteFromTableWhere(jdbcTemplate, "customers", "first_name = ?", "Test666");

		messages = drain(messageCollector.forChannel(channels.output()));

		assertThat(messages.size(),
				is((!flatteringProps.isDropTombstones() && tombstoneSupport.isKafkaPresent()) ? 4 : 3));

		assertJsonEquals(resourceToString("classpath:/json/mysql_flattered_update_inventory_customers.json"),
				messages.get(1).getPayload().toString());

		assertEquals("my-app-connector.inventory.customers", messages.get(1).getHeaders().get("cdc_topic"));
		assertJsonEquals("{\"id\":" + newRecordId + "}", messages.get(1).getHeaders().get("cdc_key"));
		if (flatteringProps.isOperationHeader()) {
			assertEquals("u", messages.get(1).getHeaders().get("cdc_operation"));
		}

		if (flatteringProps.getDeleteHandlingMode() == CdcCommonProperties.DeleteHandlingMode.none) {
			assertEquals("null", messages.get(2).getPayload().toString());
			assertEquals("my-app-connector.inventory.customers", messages.get(1).getHeaders().get("cdc_topic"));
			assertJsonEquals("{\"id\":" + newRecordId + "}", messages.get(1).getHeaders().get("cdc_key"));
			if (flatteringProps.isOperationHeader()) {
				assertEquals("d", messages.get(2).getHeaders().get("cdc_operation"));
			}
		}

		if (!flatteringProps.isDropTombstones() && tombstoneSupport.isKafkaPresent()) {
			assertThat("Tombstones event should have KafkaNull payload",
					messages.get(3).getPayload().getClass().getCanonicalName(),
					is(ORG_SPRINGFRAMEWORK_KAFKA_SUPPORT_KAFKA_NULL));
			assertEquals("my-app-connector.inventory.customers", messages.get(3).getHeaders().get("cdc_topic"));
			assertJsonEquals("{\"id\":" + newRecordId + "}", messages.get(3).getHeaders().get("cdc_key"));
		}
	};

}
