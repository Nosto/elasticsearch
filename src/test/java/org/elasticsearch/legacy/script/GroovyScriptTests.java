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

package org.elasticsearch.legacy.script;

import org.elasticsearch.legacy.ExceptionsHelper;
import org.elasticsearch.legacy.action.index.IndexRequestBuilder;
import org.elasticsearch.legacy.action.search.SearchPhaseExecutionException;
import org.elasticsearch.legacy.action.search.SearchResponse;
import org.elasticsearch.legacy.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.legacy.index.query.FilterBuilders.scriptFilter;
import static org.elasticsearch.legacy.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.legacy.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

/**
 * Various tests for Groovy scripting
 */
public class GroovyScriptTests extends ElasticsearchIntegrationTest {

    @Test
    public void testGroovyBigDecimalTransformation() {
        client().prepareIndex("test", "doc", "1").setSource("foo", 5).setRefresh(true).get();

        // Test that something that would usually be a BigDecimal is transformed into a Double
        assertScript("def n = 1.23; assert n instanceof Double;");
        assertScript("def n = 1.23G; assert n instanceof Double;");
        assertScript("def n = BigDecimal.ONE; assert n instanceof BigDecimal;");
    }

    public void assertScript(String script) {
        SearchResponse resp = client().prepareSearch("test")
                .setSource("{\"query\": {\"match_all\": {}}," +
                        "\"sort\":{\"_script\": {\"script\": \""+ script +
                        "; 1\", \"type\": \"number\", \"lang\": \"groovy\"}}}").get();
        assertNoFailures(resp);
    }

    @Test
    public void testGroovyExceptionSerialization() throws Exception {
        List<IndexRequestBuilder> reqs = newArrayList();
        for (int i = 0; i < randomIntBetween(50, 500); i++) {
            reqs.add(client().prepareIndex("test", "doc", "" + i).setSource("foo", "bar"));
        }
        indexRandom(true, false, reqs);
        try {
            client().prepareSearch("test").setQuery(constantScoreQuery(scriptFilter("1 == not_found").lang("groovy"))).get();
            fail("should have thrown an exception");
        } catch (SearchPhaseExecutionException e) {
            assertThat(ExceptionsHelper.detailedMessage(e) + "should not contained NotSerializableTransportException",
                    ExceptionsHelper.detailedMessage(e).contains("NotSerializableTransportException"), equalTo(false));
            assertThat(ExceptionsHelper.detailedMessage(e) + "should have contained GroovyScriptExecutionException",
                    ExceptionsHelper.detailedMessage(e).contains("GroovyScriptExecutionException"), equalTo(true));
            assertThat(ExceptionsHelper.detailedMessage(e) + "should have contained not_found",
                    ExceptionsHelper.detailedMessage(e).contains("No such property: not_found"), equalTo(true));
        }

        try {
            client().prepareSearch("test").setQuery(constantScoreQuery(
                    scriptFilter("pr = Runtime.getRuntime().exec(\"touch /tmp/gotcha\"); pr.waitFor()").lang("groovy"))).get();
            fail("should have thrown an exception");
        } catch (SearchPhaseExecutionException e) {
            assertThat(ExceptionsHelper.detailedMessage(e) + "should not contained NotSerializableTransportException",
                    ExceptionsHelper.detailedMessage(e).contains("NotSerializableTransportException"), equalTo(false));
            assertThat(ExceptionsHelper.detailedMessage(e) + "should have contained GroovyScriptCompilationException",
                    ExceptionsHelper.detailedMessage(e).contains("GroovyScriptCompilationException"), equalTo(true));
            assertThat(ExceptionsHelper.detailedMessage(e) + "should have contained Method calls not allowed on [java.lang.Runtime]",
                    ExceptionsHelper.detailedMessage(e).contains("Method calls not allowed on [java.lang.Runtime]"), equalTo(true));
        }

        try {
            client().prepareSearch("test").setQuery(constantScoreQuery(
                    scriptFilter("assert false").lang("groovy"))).get();
            fail("should have thrown an exception");
        } catch (SearchPhaseExecutionException e) {
            assertThat(ExceptionsHelper.detailedMessage(e) + "should not contained NotSerializableTransportException",
                    ExceptionsHelper.detailedMessage(e).contains("NotSerializableTransportException"), equalTo(false));
            assertThat(ExceptionsHelper.detailedMessage(e) + "should have contained GroovyScriptExecutionException",
                    ExceptionsHelper.detailedMessage(e).contains("GroovyScriptExecutionException"), equalTo(true));
            assertThat(ExceptionsHelper.detailedMessage(e) + "should have contained an assert error",
                    ExceptionsHelper.detailedMessage(e).contains("PowerAssertionError[assert false"), equalTo(true));
        }
    }
}
