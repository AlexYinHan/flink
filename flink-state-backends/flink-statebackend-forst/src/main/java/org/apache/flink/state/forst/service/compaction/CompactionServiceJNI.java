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

import org.apache.flink.state.forst.fs.ForStFlinkFileSystem;
import org.apache.flink.state.forst.fs.StringifiedForStFileSystem;

import java.io.IOException;

public class CompactionServiceJNI {
    public static native Object handleCompactionRequest(
            byte[] params, byte[] serializedFileMappings);

    public static ForStFlinkFileSystem getForStFlinkFileSystemFromObject(Object fsObject) {
        if (fsObject instanceof ForStFlinkFileSystem) {
            return (ForStFlinkFileSystem) fsObject;
        } else if (fsObject instanceof StringifiedForStFileSystem) {
            return ((StringifiedForStFileSystem) fsObject).getFileSystem();
        } else {
            throw new RuntimeException("unsupported fileSystem: " + fsObject);
        }
    }

    public static void registerFileMappings(Object fsObject, byte[] serializedFileMappings)
            throws IOException, ClassNotFoundException {
        ForStFlinkFileSystem forStFlinkFileSystem = getForStFlinkFileSystemFromObject(fsObject);
        forStFlinkFileSystem.buildFromBytes(serializedFileMappings);
    }

    public static void registerOutput(Object fsObject, byte[] compactionOutputBytes) {
        ForStFlinkFileSystem forStFlinkFileSystem = getForStFlinkFileSystemFromObject(fsObject);
        forStFlinkFileSystem.registerCompactionOutput(compactionOutputBytes);
    }
}
