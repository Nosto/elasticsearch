/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.legacy.cluster;

import org.elasticsearch.legacy.common.unit.TimeValue;

/**
 * An extension interface to {@link org.elasticsearch.legacy.cluster.ClusterStateUpdateTask} that allows to associate
 * a timeout.
 */
public interface TimeoutClusterStateUpdateTask extends ProcessedClusterStateUpdateTask {

    /**
     * If the cluster state update task wasn't processed by the provided timeout, call
     * {@link #onFailure(String, Throwable)}
     */
    TimeValue timeout();
}
