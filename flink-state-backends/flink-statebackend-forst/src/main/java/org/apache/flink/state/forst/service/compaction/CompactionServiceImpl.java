/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst.service.compaction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.state.forst.fs.ForStFlinkFileSystem;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.apache.dubbo.config.bootstrap.builders.ServiceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class CompactionServiceImpl implements CompactionService {
    private static final Logger LOG = LoggerFactory.getLogger(CompactionServiceImpl.class);

    @Override
    public void ping() {}

    @Override
    public Tuple2<byte[], byte[]> performCompaction(byte[] params, byte[] serializedFileMappings) {
        UUID uuid = UUID.randomUUID();
        AtomicReference<Tuple2<byte[], byte[]>> output = new AtomicReference<>();
        Thread t =
                new Thread(
                        () -> {
                            try {
                                LOG.info("perform compaction on compaction-service side, {}", uuid);
                                Object fsObject =
                                        CompactionServiceJNI.handleCompactionRequest(
                                                params, serializedFileMappings);
                                ForStFlinkFileSystem forStFlinkFileSystem =
                                        CompactionServiceJNI.getForStFlinkFileSystemFromObject(
                                                fsObject);
                                output.set(forStFlinkFileSystem.getCompactionOutput());
                            } catch (Exception e) {
                                LOG.info(
                                        "perform compaction on compaction-service side failed, {}",
                                        uuid);
                                throw new RuntimeException(e);
                            }
                        });
        t.setName("FlinkRemoteCompaction" + uuid);
        t.start();
        try {
            t.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return output.get();
    }

    private static DubboBootstrap serviceInstance = null;

    public static void startService() {
        if (serviceInstance != null) {
            return;
        }

        serviceInstance =
                DubboBootstrap.getInstance()
                        .protocol(new ProtocolConfig(CommonConstants.TRIPLE, 50051))
                        .service(
                                ServiceBuilder.newBuilder()
                                        .interfaceClass(CompactionService.class)
                                        .ref(new CompactionServiceImpl())
                                        .timeout(3600000)
                                        .build());
        serviceInstance.start();
    }

    public static void stopService() {
        serviceInstance.stop();
        serviceInstance = null;
    }
}
