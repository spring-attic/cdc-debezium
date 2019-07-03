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

package org.springframework.cloud.stream.app.cdc.source;

import java.util.function.Consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.app.cdc.common.core.CdcStreamConfiguration;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.reactive.StreamEmitter;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.Message;

/**
 * CDC source that uses the Debezium Connectors to monitor and record all of the row-level changes in the databases.
 *
 * https://debezium.io/docs/connectors
 *
 * @author Christian Tzolov
 */
@EnableBinding(Source.class)
@EnableConfigurationProperties({ CdcSourceProperties.class })
@Import(CdcStreamConfiguration.class)
public class CdcDebeziumSourceConfiguration {

	private static final Log logger = LogFactory.getLog(CdcDebeziumSourceConfiguration.class);

	@Autowired
	private Consumer<FluxSink<Message<byte[]>>> engine;

	@Autowired
	private CdcSourceProperties properties;

	@StreamEmitter
	@Output(Source.OUTPUT)
	public Flux<Message<byte[]>> emit() {
		return Flux.create(engine);
	}
}
