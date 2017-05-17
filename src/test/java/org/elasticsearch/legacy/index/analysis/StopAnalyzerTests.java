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

package org.elasticsearch.legacy.index.analysis;

import org.elasticsearch.legacy.common.inject.Injector;
import org.elasticsearch.legacy.common.inject.ModulesBuilder;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.common.settings.SettingsModule;
import org.elasticsearch.legacy.env.Environment;
import org.elasticsearch.legacy.env.EnvironmentModule;
import org.elasticsearch.legacy.index.Index;
import org.elasticsearch.legacy.index.IndexNameModule;
import org.elasticsearch.legacy.index.settings.IndexSettingsModule;
import org.elasticsearch.legacy.indices.analysis.IndicesAnalysisModule;
import org.elasticsearch.legacy.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.legacy.test.ElasticsearchTokenStreamTestCase;
import org.junit.Test;

import static org.elasticsearch.legacy.common.settings.ImmutableSettings.settingsBuilder;

public class StopAnalyzerTests extends ElasticsearchTokenStreamTestCase {

    @Test
    public void testDefaultsCompoundAnalysis() throws Exception {
        Index index = new Index("test");
        Settings settings = settingsBuilder().loadFromClasspath("org/elasticsearch/legacy/index/analysis/stop.json").build();
        Injector parentInjector = new ModulesBuilder().add(new SettingsModule(settings), new EnvironmentModule(new Environment(settings)), new IndicesAnalysisModule()).createInjector();
        Injector injector = new ModulesBuilder().add(
                new IndexSettingsModule(index, settings),
                new IndexNameModule(index),
                new AnalysisModule(settings, parentInjector.getInstance(IndicesAnalysisService.class)))
                .createChildInjector(parentInjector);

        AnalysisService analysisService = injector.getInstance(AnalysisService.class);

        NamedAnalyzer analyzer1 = analysisService.analyzer("analyzer1");

        assertTokenStreamContents(analyzer1.tokenStream("test", "to be or not to be"), new String[0]);

        NamedAnalyzer analyzer2 = analysisService.analyzer("analyzer2");

        assertTokenStreamContents(analyzer2.tokenStream("test", "to be or not to be"), new String[0]);
    }

}
