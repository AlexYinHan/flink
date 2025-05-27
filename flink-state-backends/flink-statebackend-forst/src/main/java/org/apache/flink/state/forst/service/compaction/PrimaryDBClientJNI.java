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
import org.apache.flink.state.forst.fs.StringifiedForStFileSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrimaryDBClientJNI {

    private static final Logger LOG = LoggerFactory.getLogger(PrimaryDBClientJNI.class);

    static native void handleCompactionResponse(byte[] output, long ongoingCompactionHandle);

    public static void invokeCompactionService(
            byte[] params, String inputFiles, Object fileSystem, long ongoingCompactionHandle) {
        try {
            LOG.info("invokeCompactionService, input files: {}", inputFiles);
            CompactionService compactionService = PrimaryDBClient.getCompactionService();
            ForStFlinkFileSystem forStFlinkFileSystem;
            if (fileSystem instanceof ForStFlinkFileSystem) {
                forStFlinkFileSystem = (ForStFlinkFileSystem) fileSystem;
            } else if (fileSystem instanceof StringifiedForStFileSystem) {
                forStFlinkFileSystem = ((StringifiedForStFileSystem) fileSystem).getFileSystem();
            } else {
                throw new RuntimeException("unsupported fileSystem: " + fileSystem);
            }
            Tuple2<byte[], byte[]> outputAndFileMapping =
                    compactionService.performCompaction(
                            params,
                            forStFlinkFileSystem.getSerializedMappingTableForCompactionParams(
                                    inputFiles));
            LOG.info("invokeCompactionService complete: " + outputAndFileMapping.f0.length);

            forStFlinkFileSystem.buildFromBytes(outputAndFileMapping.f1);
            handleCompactionResponse(outputAndFileMapping.f0, ongoingCompactionHandle);
            LOG.info("install outputs complete: " + outputAndFileMapping.f0.length);
        } catch (Exception e) {
            LOG.error("invokeCompactionService error", e);
        }
    }
}
