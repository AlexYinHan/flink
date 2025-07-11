/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;

import javax.annotation.Nullable;

/**
 * Extension of {@link TwoInputStreamOperatorTestHarness} that allows the operator to get a {@link
 * KeyedStateBackend}.
 */
public class KeyedTwoInputStreamOperatorTestHarness<K, IN1, IN2, OUT>
        extends TwoInputStreamOperatorTestHarness<IN1, IN2, OUT> {

    public KeyedTwoInputStreamOperatorTestHarness(
            TwoInputStreamOperator<IN1, IN2, OUT> operator,
            KeySelector<IN1, K> keySelector1,
            KeySelector<IN2, K> keySelector2,
            TypeInformation<K> keyType,
            int maxParallelism,
            int numSubtasks,
            int subtaskIndex)
            throws Exception {
        this(
                operator,
                keySelector1,
                keySelector2,
                keyType,
                maxParallelism,
                numSubtasks,
                subtaskIndex,
                null,
                null);
    }

    public KeyedTwoInputStreamOperatorTestHarness(
            TwoInputStreamOperator<IN1, IN2, OUT> operator,
            KeySelector<IN1, K> keySelector1,
            KeySelector<IN2, K> keySelector2,
            TypeInformation<K> keyType,
            int maxParallelism,
            int numSubtasks,
            int subtaskIndex,
            @Nullable TypeSerializer<?> leftSerializer,
            @Nullable TypeSerializer<?> rightSerializer)
            throws Exception {
        super(operator, maxParallelism, numSubtasks, subtaskIndex);

        ClosureCleaner.clean(keySelector1, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
        ClosureCleaner.clean(keySelector2, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
        config.setStatePartitioner(0, keySelector1);
        config.setStatePartitioner(1, keySelector2);
        config.setStateKeySerializer(
                keyType.createSerializer(executionConfig.getSerializerConfig()));
        if (leftSerializer != null && rightSerializer != null) {
            config.setupNetworkInputs(leftSerializer, rightSerializer);
        }
        config.serializeAllConfigs();
    }

    public KeyedTwoInputStreamOperatorTestHarness(
            TwoInputStreamOperator<IN1, IN2, OUT> operator,
            final KeySelector<IN1, K> keySelector1,
            final KeySelector<IN2, K> keySelector2,
            TypeInformation<K> keyType)
            throws Exception {
        this(operator, keySelector1, keySelector2, keyType, 1, 1, 0);
    }

    public int numKeyedStateEntries() {
        AbstractStreamOperator<?> abstractStreamOperator = (AbstractStreamOperator<?>) operator;
        KeyedStateBackend<Object> keyedStateBackend = abstractStreamOperator.getKeyedStateBackend();
        if (keyedStateBackend instanceof HeapKeyedStateBackend) {
            return ((HeapKeyedStateBackend) keyedStateBackend).numKeyValueStateEntries();
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported keyed state backend: %s",
                            keyedStateBackend.getClass().getCanonicalName()));
        }
    }

    public void endAllInputs() throws Exception {
        TwoInputStreamOperator<IN1, IN2, OUT> op = (TwoInputStreamOperator<IN1, IN2, OUT>) operator;
        if (op instanceof BoundedMultiInput) {
            ((BoundedMultiInput) op).endInput(1);
            ((BoundedMultiInput) op).endInput(2);
        }
    }
}
