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

import java.io.IOException;
import java.util.UUID;

public class CompactionServiceImpl implements CompactionService {
    private static final Logger LOG = LoggerFactory.getLogger(CompactionServiceImpl.class);

    @Override
    public void ping() {
        //        LOG.info("ping on compaction-service side");
        //        System.out.println("ping on compaction-service side");
    }

    @Override
    public Tuple2<byte[], byte[]> performCompaction(byte[] params, byte[] serializedFileMappings) {
        UUID uuid = UUID.randomUUID();
        //        LOG.info("here, {}", uuid);

        LOG.info("perform compaction on compaction-service side, {}", uuid);
        //        System.out.println("perform compaction on compaction-service side1: " +
        // params.length);
        Object fsObject =
                CompactionServiceJNI.handleCompactionRequest(params, serializedFileMappings);
        //        System.out.println("perform compaction on compaction-service side2: " +
        // params.length);
        ForStFlinkFileSystem forStFlinkFileSystem =
                CompactionServiceJNI.getForStFlinkFileSystemFromObject(fsObject);
        //        System.out.println("perform compaction on compaction-service side3: " +
        // params.length);
        //        System.out.println(
        //                "perform compaction on compaction-service side complete: " +
        // params.length);

        //        LOG.info("return, {}", uuid);
        try {
            return forStFlinkFileSystem.getFileMappingManager().getCompactionOutput();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        //        System.out.println("start service1");
        serviceInstance.start();
    }

    public static void stopService() {
        serviceInstance.stop();
        serviceInstance = null;
    }
}
