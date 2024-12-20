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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.testutils.ClassLoaderUtils;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TernaryBoolean;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This test validates that the checkpoint settings serialize correctly in the presence of
 * user-defined objects.
 */
class CheckpointSettingsSerializableTest {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testDeserializationOfUserCodeWithUserClassLoader() throws Exception {
        final ClassLoaderUtils.ObjectAndClassLoader<Serializable> outsideClassLoading =
                ClassLoaderUtils.createSerializableObjectFromNewClassLoader();
        final ClassLoader classLoader = outsideClassLoading.getClassLoader();
        final Serializable outOfClassPath = outsideClassLoading.getObject();

        final MasterTriggerRestoreHook.Factory[] hooks = {new TestFactory(outOfClassPath)};
        final SerializedValue<MasterTriggerRestoreHook.Factory[]> serHooks =
                new SerializedValue<>(hooks);

        final JobCheckpointingSettings checkpointingSettings =
                new JobCheckpointingSettings(
                        new CheckpointCoordinatorConfiguration(
                                1000L,
                                10000L,
                                0L,
                                1,
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
                                true,
                                false,
                                0,
                                0),
                        new SerializedValue<StateBackend>(new CustomStateBackend(outOfClassPath)),
                        TernaryBoolean.UNDEFINED,
                        new SerializedValue<>(new CustomCheckpointStorage(outOfClassPath)),
                        serHooks);

        final JobGraph jobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder()
                        .setJobCheckpointingSettings(checkpointingSettings)
                        .build();

        // to serialize/deserialize the job graph to see if the behavior is correct under
        // distributed execution
        final JobGraph copy = CommonTestUtils.createCopySerializable(jobGraph);

        final ExecutionGraph eg =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setJobGraph(copy)
                        .setUserClassLoader(classLoader)
                        .build(EXECUTOR_EXTENSION.getExecutor());

        assertThat(eg.getCheckpointCoordinator().getNumberOfRegisteredMasterHooks()).isOne();
        assertThat(
                        jobGraph.getCheckpointingSettings()
                                .getDefaultStateBackend()
                                .deserializeValue(classLoader))
                .isInstanceOf(CustomStateBackend.class);
    }

    // ------------------------------------------------------------------------

    private static final class TestFactory implements MasterTriggerRestoreHook.Factory {

        private static final long serialVersionUID = -612969579110202607L;

        private final Serializable payload;

        TestFactory(Serializable payload) {
            this.payload = payload;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <V> MasterTriggerRestoreHook<V> create() {
            MasterTriggerRestoreHook<V> hook = mock(MasterTriggerRestoreHook.class);
            when(hook.getIdentifier()).thenReturn("id");
            return hook;
        }
    }

    private static final class CustomStateBackend implements StateBackend {

        private static final long serialVersionUID = -6107964383429395816L;

        /** Simulate a custom option that is not in the normal classpath. */
        @SuppressWarnings("unused")
        private Serializable customOption;

        public CustomStateBackend(Serializable customOption) {
            this.customOption = customOption;
        }

        @Override
        public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
                KeyedStateBackendParameters<K> parameters) {
            throw new UnsupportedOperationException();
        }

        @Override
        public OperatorStateBackend createOperatorStateBackend(
                OperatorStateBackendParameters parameters) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class CustomCheckpointStorage implements CheckpointStorage {

        private static final long serialVersionUID = -6107964383429395816L;

        /** Simulate a custom option that is not in the normal classpath. */
        @SuppressWarnings("unused")
        private Serializable customOption;

        public CustomCheckpointStorage(Serializable customOption) {
            this.customOption = customOption;
        }

        @Override
        public CompletedCheckpointStorageLocation resolveCheckpoint(String pointer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CheckpointStorageAccess createCheckpointStorage(JobID jobId) throws IOException {
            return mock(CheckpointStorageAccess.class);
        }
    }
}
