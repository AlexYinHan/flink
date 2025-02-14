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

export interface JobStatusCounts {
  CREATED: number;
  SCHEDULED: number;
  CANCELED: number;
  DEPLOYING: number;
  RUNNING: number;
  CANCELING: number;
  FINISHED: number;
  FAILED: number;
  RECONCILING: number;
  PENDING: number;
}

interface TimestampsStatus {
  FINISHED: number;
  FAILING: number;
  SUSPENDING: number;
  RECONCILING: number;
  CREATED: number;
  RESTARTING: number;
  CANCELLING: number;
  FAILED: number;
  CANCELED: number;
  RUNNING: number;
  SUSPENDED: number;
}

export interface JobDetail {
  jid: string;
  name: string;
  isStoppable: boolean;
  state: string;
  'job-type': string;
  'start-time': number;
  'end-time': number;
  duration: number;
  maxParallelism: number;
  now: number;
  timestamps: TimestampsStatus;
  vertices: VerticesItem[];
  'status-counts': JobStatusCounts;
  plan: Plan;
  'stream-graph': StreamGraph;
  'pending-operators': number;
}

interface Plan {
  jid: string;
  name: string;
  type: string;
  nodes: NodesItem[];
}

interface StreamGraph {
  nodes: NodesItemCorrect[];
}

interface InputsItem {
  num: number;
  id: string;
  ship_strategy: string;
  exchange: string;
}

export interface VerticesLink extends InputsItem {
  source: string;
  target: string;
  id: string;
}

export interface VerticesItem {
  id: string;
  name: string;
  parallelism: number;
  maxParallelism: number;
  status: string;
  'start-time': number;
  'end-time': number;
  duration: number;
  tasks: TasksStatus;
  metrics: MetricsStatus;
}

export interface VerticesItemRange extends VerticesItem {
  range: number[];
}

export interface TasksStatus {
  FINISHED: number;
  SCHEDULED: number;
  CANCELED: number;
  CREATED: number;
  DEPLOYING: number;
  RUNNING: number;
  FAILED: number;
  RECONCILING: number;
  CANCELING: number;
  INITIALIZING: number;
  PENDING: number;
}

interface MetricsStatus {
  'read-bytes': number;
  'read-bytes-complete': boolean;
  'write-bytes': number;
  'write-bytes-complete': boolean;
  'read-records': number;
  'read-records-complete': boolean;
  'write-records': number;
  'write-records-complete': boolean;
}

export interface NodesItem {
  id: string;
  parallelism: number;
  operator: string;
  operator_strategy: string;
  description: string;
  inputs?: InputsItem[];
  optimizer_properties: unknown;
  width?: number;
  height?: number;
}

export interface NodesItemCorrect extends NodesItem {
  detail: VerticesItem | undefined;
  lowWatermark?: number;
  backPressuredPercentage?: number;
  busyPercentage?: number;
  dataSkewPercentage?: number;
  job_vertex_id?: string;
}

export interface NodesItemLink {
  id: string;
  source: string;
  target: string;
  width?: number;
  ship_strategy?: string;
  local_strategy?: string;
  pending?: boolean;
}

export interface JobDetailCorrect extends JobDetail {
  plan: {
    jid: string;
    name: string;
    type: string;
    nodes: NodesItemCorrect[];
    links: NodesItemLink[];
    streamNodes: NodesItemCorrect[];
    streamLinks: NodesItemLink[];
  };
}
