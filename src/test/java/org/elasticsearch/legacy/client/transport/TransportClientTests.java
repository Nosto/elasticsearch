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

package org.elasticsearch.legacy.client.transport;

import org.elasticsearch.legacy.common.settings.ImmutableSettings;
import org.elasticsearch.legacy.test.ElasticsearchIntegrationTest;
import org.elasticsearch.legacy.test.ElasticsearchIntegrationTest.ClusterScope;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.elasticsearch.legacy.test.ElasticsearchIntegrationTest.*;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, transportClientRatio = 1.0)
public class TransportClientTests extends ElasticsearchIntegrationTest {

    @Test
    public void testPickingUpChangesInDiscoveryNode() {
        String nodeName = internalCluster().startNode(ImmutableSettings.builder().put("node.data", false));

        TransportClient client = (TransportClient) internalCluster().client(nodeName);
        assertThat(client.connectedNodes().get(0).dataNode(), Matchers.equalTo(false));

    }
}
