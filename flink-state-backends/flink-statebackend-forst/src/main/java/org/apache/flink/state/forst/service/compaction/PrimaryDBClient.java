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

import org.apache.dubbo.config.bootstrap.builders.ReferenceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrimaryDBClient implements ForStRPCEndpoint {
    private static final Logger LOG = LoggerFactory.getLogger(PrimaryDBClient.class);
    private static String kCompactionServiceAddress = "tri://192.168.0.251:50051";
    private static CompactionService compactionService;

    public static synchronized void setCompactionServiceAddress(String compactionServiceAddress) {
        if (compactionService != null) {
            return;
        }
        kCompactionServiceAddress = compactionServiceAddress;
        compactionService = getCompactionService();
    }

    public static synchronized CompactionService getCompactionService() {
        return compactionService == null ? initCompactionService() : compactionService;
    }

    public static synchronized CompactionService initCompactionService() {
        LOG.info("Getting compaction service from {}", kCompactionServiceAddress);
        CompactionService compactionService =
                (CompactionService)
                        ReferenceBuilder.newBuilder()
                                .interfaceClass(CompactionService.class)
                                .url(kCompactionServiceAddress)
                                .timeout(3600000)
                                .build()
                                .get();
        compactionService.ping();
        LOG.info("Got compaction service from {}", kCompactionServiceAddress);
        return compactionService;
    }
}
