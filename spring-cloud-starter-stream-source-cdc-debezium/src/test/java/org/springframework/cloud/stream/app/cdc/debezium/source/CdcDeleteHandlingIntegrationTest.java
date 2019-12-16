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
import java.util.concurrent.TimeUnit;

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

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.springframework.cloud.stream.app.cdc.common.core.CdcTombstoneConfiguration.ORG_SPRINGFRAMEWORK_KAFKA_SUPPORT_KAFKA_NULL;
import static org.springframework.cloud.stream.app.cdc.debezium.source.CdcTestUtils.drain;
import static org.springframework.cloud.stream.app.cdc.debezium.source.CdcTestUtils.jdbcTemplate;

/**
 * @author Christian Tzolov
 */
public class CdcDeleteHandlingIntegrationTest {

	private final JdbcTemplate jdbcTemplate = jdbcTemplate(
			"com.mysql.cj.jdbc.Driver",
			"jdbc:mysql://localhost:3306/inventory",
			"root", "debezium");

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
			.withUserConfiguration(TestCdcSourceApplication.class)
			.withPropertyValues(
					"cdc.name=my-sql-connector",
					"cdc.schema=false",
					"cdc.flattering.enabled=true",
					"cdc.stream.header.offset=true",
					"cdc.connector=mysql",
					"cdc.config.database.user=debezium",
					"cdc.config.database.password=dbz",
					"cdc.config.database.hostname=localhost",
					"cdc.config.database.port=3306",
					"cdc.config.database.server.id=85744",
					"cdc.config.database.server.name=my-app-connector",
					"cdc.config.database.history=io.debezium.relational.history.MemoryDatabaseHistory");

	@Test
	public void handleRecordDeletionTest() {
		contextRunner.withPropertyValues("cdc.flattering.deleteHandlingMode=none", "cdc.flattering.dropTombstones=true")
				.run(consumer);
		contextRunner.withPropertyValues("cdc.flattering.deleteHandlingMode=none", "cdc.flattering.dropTombstones=true")
				.withClassLoader(new FilteredClassLoader(KafkaNull.class)) // Remove Kafka from the classpath
				.run(consumer);

		contextRunner.withPropertyValues("cdc.flattering.deleteHandlingMode=none", "cdc.flattering.dropTombstones=false")
				.run(consumer);
		contextRunner.withPropertyValues("cdc.flattering.deleteHandlingMode=none", "cdc.flattering.dropTombstones=false")
				.withClassLoader(new FilteredClassLoader(KafkaNull.class)) // Remove Kafka from the classpath
				.run(consumer);

		contextRunner.withPropertyValues("cdc.flattering.deleteHandlingMode=drop", "cdc.flattering.dropTombstones=true")
				.run(consumer);
		contextRunner.withPropertyValues("cdc.flattering.deleteHandlingMode=drop", "cdc.flattering.dropTombstones=true")
				.withClassLoader(new FilteredClassLoader(KafkaNull.class)) // Remove Kafka from the classpath
				.run(consumer);

		contextRunner.withPropertyValues("cdc.flattering.deleteHandlingMode=drop", "cdc.flattering.dropTombstones=false")
				.run(consumer);
		contextRunner.withPropertyValues("cdc.flattering.deleteHandlingMode=drop", "cdc.flattering.dropTombstones=false")
				.withClassLoader(new FilteredClassLoader(KafkaNull.class)) // Remove Kafka from the classpath
				.run(consumer);

		contextRunner.withPropertyValues("cdc.flattering.deleteHandlingMode=rewrite", "cdc.flattering.dropTombstones=true")
				.run(consumer);
		contextRunner.withPropertyValues("cdc.flattering.deleteHandlingMode=rewrite", "cdc.flattering.dropTombstones=true")
				.withClassLoader(new FilteredClassLoader(KafkaNull.class)) // Remove Kafka from the classpath
				.run(consumer);

		contextRunner.withPropertyValues("cdc.flattering.deleteHandlingMode=rewrite", "cdc.flattering.dropTombstones=false")
				.run(consumer);
		contextRunner.withPropertyValues("cdc.flattering.deleteHandlingMode=rewrite", "cdc.flattering.dropTombstones=false")
				.withClassLoader(new FilteredClassLoader(KafkaNull.class)) // Remove Kafka from the classpath
				.run(consumer);
	}

	final ContextConsumer<? super AssertableApplicationContext> consumer = context -> {
		Source channels = context.getBean(Source.class);
		MessageCollector messageCollector = context.getBean(MessageCollector.class);
		CdcCommonProperties props = context.getBean(CdcCommonProperties.class);
		CdcTombstoneConfiguration.TombstoneSupport tombstoneSupport = context.getBean(CdcTombstoneConfiguration.TombstoneSupport.class);
		CdcCommonProperties.DeleteHandlingMode deleteHandlingMode = props.getFlattering().getDeleteHandlingMode();
		boolean isDropTombstones = props.getFlattering().isDropTombstones();

		jdbcTemplate.update("insert into `customers`(`first_name`,`last_name`,`email`) VALUES('Test666', 'Test666', 'Test666@spring.org')");
		String newRecordId = jdbcTemplate.query("select * from `customers` where `first_name` = ?",
				(rs, rowNum) -> rs.getString("id"), "Test666").iterator().next();

		List<Message<?>> messages = drain(messageCollector.forChannel(channels.output()));
		assertThat(messages.size(), is(53));

		JdbcTestUtils.deleteFromTableWhere(jdbcTemplate, "customers", "first_name = ?", "Test666");

		Message<?> received;
		switch (deleteHandlingMode) {
		case drop: {
			break;
		}
		case none: {
			received = messageCollector.forChannel(channels.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat(received.getPayload().toString(), is("null"));
			break;
		}
		case rewrite: {
			received = messageCollector.forChannel(channels.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat(received.getPayload().toString(), containsString("\"__deleted\":\"true\""));
			break;
		}
		}

		if (isDropTombstones == false && tombstoneSupport.isKafkaPresent()) {
			received = messageCollector.forChannel(channels.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat("Tombstones event should have KafkaNull payload",
					received.getPayload().getClass().getCanonicalName(), is(ORG_SPRINGFRAMEWORK_KAFKA_SUPPORT_KAFKA_NULL));
			String key = (String) received.getHeaders().get("cdc_key");
			assertThat("Tombstones event should carry the deleted record id in the cdc_key header",
					key, is("{\"id\":" + newRecordId + "}"));
		}

		received = messageCollector.forChannel(channels.output()).poll(10, TimeUnit.SECONDS);
		assertNull(received);
	};

}
