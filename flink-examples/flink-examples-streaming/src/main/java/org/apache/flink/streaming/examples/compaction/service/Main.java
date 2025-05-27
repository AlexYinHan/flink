/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package org.apache.flink.streaming.examples.compaction.service;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Main class for compaction service. */
public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = new Configuration();
        configuration.set(StateBackendOptions.STATE_BACKEND, "forst");

        configuration.set(
                ConfigOptions.key("embedCompactionService").booleanType().noDefaultValue(), true);
        env.getConfig().setEmbedCompactionService(true);

        env.configure(configuration);
        env.getConfig().setGlobalJobParameters(configuration);

        String jobName = "CompactionServiceHolder";

        DataStream<Long> source =
                env.addSource(
                        new SourceFunction<Long>() {
                            @Override
                            public void run(SourceContext<Long> ctx) throws Exception {
                                while (true) {
                                    ctx.collect(1L);
                                    Thread.sleep(1000);
                                }
                            }

                            @Override
                            public void cancel() {}
                        });
        DataStream<Long> mapper =
                source.keyBy(e -> e)
                        .flatMap(
                                new FlatMapFunction<Long, Long>() {
                                    @Override
                                    public void flatMap(Long value, Collector<Long> out)
                                            throws Exception {
                                        out.collect(value);
                                    }
                                })
                        .setParallelism(1);

        mapper.addSink(
                        new RichSinkFunction<>() {
                            @Override
                            public void setRuntimeContext(RuntimeContext t) {
                                super.setRuntimeContext(t);
                            }
                        })
                .setParallelism(1);
        env.execute(jobName);
    }
}
