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

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;

/**
 * Based on {@link SourceFieldMapper}
 */
public class UpdateSourceFieldMapper extends MetadataFieldMapper {
    public static final String NAME = "_update_source";
    public static final String CONTENT_TYPE = "_update_source";

    private final Function<Map<String, ?>, Map<String, Object>> filter;

    public static class Defaults {
        public static final String NAME = UpdateSourceFieldMapper.NAME;
        public static final boolean ENABLED = false;

        public static final MappedFieldType FIELD_TYPE = new UpdateSourceFieldMapper.SourceFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE); // not indexed
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setName(NAME);
            FIELD_TYPE.freeze();
        }

    }

    public static class Builder extends MetadataFieldMapper.Builder<UpdateSourceFieldMapper.Builder, UpdateSourceFieldMapper> {

        private boolean enabled = UpdateSourceFieldMapper.Defaults.ENABLED;

        private String[] includes = null;
        private String[] excludes = null;

        public Builder() {
            super(UpdateSourceFieldMapper.Defaults.NAME, UpdateSourceFieldMapper.Defaults.FIELD_TYPE, UpdateSourceFieldMapper.Defaults.FIELD_TYPE);
        }

        public UpdateSourceFieldMapper.Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public UpdateSourceFieldMapper.Builder includes(String[] includes) {
            this.includes = includes;
            return this;
        }

        public UpdateSourceFieldMapper.Builder excludes(String[] excludes) {
            this.excludes = excludes;
            return this;
        }

        @Override
        public UpdateSourceFieldMapper build(BuilderContext context) {
            return new UpdateSourceFieldMapper(enabled, includes, excludes, context.indexSettings());
        }
    }

    @SuppressWarnings("unchecked")
    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder<?,?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            UpdateSourceFieldMapper.Builder builder = new UpdateSourceFieldMapper.Builder();

            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    builder.enabled(TypeParsers.nodeBooleanValue(name, "enabled", fieldNode, parserContext));
                    iterator.remove();
                } else if (fieldName.equals("includes")) {
                    List<Object> values = (List<Object>) fieldNode;
                    String[] includes = new String[values.size()];
                    for (int i = 0; i < includes.length; i++) {
                        includes[i] = values.get(i).toString();
                    }
                    builder.includes(includes);
                    builder.enabled(true);
                    iterator.remove();
                } else if (fieldName.equals("excludes")) {
                    List<Object> values = (List<Object>) fieldNode;
                    String[] excludes = new String[values.size()];
                    for (int i = 0; i < excludes.length; i++) {
                        excludes[i] = values.get(i).toString();
                    }
                    builder.excludes(excludes);
                    builder.enabled(true);
                    iterator.remove();
                }
            }
            return builder;
        }

        @Override
        public MetadataFieldMapper getDefault(MappedFieldType fieldType, ParserContext context) {
            final Settings indexSettings = context.mapperService().getIndexSettings().getSettings();
            return new UpdateSourceFieldMapper(indexSettings);
        }
    }

    static final class SourceFieldType extends MappedFieldType {

        SourceFieldType() {}

        protected SourceFieldType(UpdateSourceFieldMapper.SourceFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new UpdateSourceFieldMapper.SourceFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new QueryShardException(context, "The _update_source field is not searchable");
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "The _update_source field is not searchable");
        }
    }

    private final boolean enabled;

    /** indicates whether the source will always exist and be complete, for use by features like the update API */
    private final boolean complete;

    private final String[] includes;
    private final String[] excludes;

    private UpdateSourceFieldMapper(Settings indexSettings) {
        this(UpdateSourceFieldMapper.Defaults.ENABLED, null, null, indexSettings);
    }

    private UpdateSourceFieldMapper(boolean enabled, String[] includes, String[] excludes, Settings indexSettings) {
        super(NAME, UpdateSourceFieldMapper.Defaults.FIELD_TYPE.clone(), UpdateSourceFieldMapper.Defaults.FIELD_TYPE, indexSettings); // Only stored.
        this.enabled = enabled;
        this.includes = includes;
        this.excludes = excludes;
        final boolean filtered = (includes != null && includes.length > 0) || (excludes != null && excludes.length > 0);
        this.filter = enabled && filtered && fieldType().stored() ? XContentMapValues.filter(includes, excludes) : null;
        this.complete = enabled && includes == null && excludes == null;
    }

    public boolean enabled() {
        return enabled;
    }

    public String[] excludes() {
        return this.excludes != null ? this.excludes : Strings.EMPTY_ARRAY;

    }

    public String[] includes() {
        return this.includes != null ? this.includes : Strings.EMPTY_ARRAY;
    }

    public boolean isComplete() {
        return complete;
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        // nothing to do here, we will call it in pre parse
        return null;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        if (!enabled) {
            return;
        }
        if (!fieldType().stored()) {
            return;
        }
        BytesReference source = context.sourceToParse().source();
        // Percolate and tv APIs may not set the source and that is ok, because these APIs will not index any data
        if (source == null) {
            return;
        }

        if (filter != null) {
            // we don't update the context source if we filter, we want to keep it as is...
            Tuple<XContentType, Map<String, Object>> mapTuple =
                XContentHelper.convertToMap(source, true, context.sourceToParse().getXContentType());
            Map<String, Object> filteredSource = filter.apply(mapTuple.v2());
            BytesStreamOutput bStream = new BytesStreamOutput();
            XContentType contentType = mapTuple.v1();
            XContentBuilder builder = XContentFactory.contentBuilder(contentType, bStream).map(filteredSource);
            builder.close();

            source = bStream.bytes();
        }
        BytesRef ref = source.toBytesRef();
        fields.add(new StoredField(fieldType().name(), ref.bytes, ref.offset, ref.length));
    }

    @Override
    protected String contentType() {
        return UpdateSourceFieldMapper.CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        // all are defaults, no need to write it at all
        if (!includeDefaults && enabled == UpdateSourceFieldMapper.Defaults.ENABLED && includes == null && excludes == null) {
            return builder;
        }
        builder.startObject(contentType());
        if (includeDefaults || enabled != UpdateSourceFieldMapper.Defaults.ENABLED) {
            builder.field("enabled", enabled);
        }

        if (includes != null) {
            builder.array("includes", includes);
        } else if (includeDefaults) {
            builder.array("includes", Strings.EMPTY_ARRAY);
        }

        if (excludes != null) {
            builder.array("excludes", excludes);
        } else if (includeDefaults) {
            builder.array("excludes", Strings.EMPTY_ARRAY);
        }

        builder.endObject();
        return builder;
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        UpdateSourceFieldMapper sourceMergeWith = (UpdateSourceFieldMapper) mergeWith;
        List<String> conflicts = new ArrayList<>();
        if (this.enabled != sourceMergeWith.enabled) {
            conflicts.add("Cannot update enabled setting for [_update_source]");
        }
        if (Arrays.equals(includes(), sourceMergeWith.includes()) == false) {
            conflicts.add("Cannot update includes setting for [_update_source]");
        }
        if (Arrays.equals(excludes(), sourceMergeWith.excludes()) == false) {
            conflicts.add("Cannot update excludes setting for [_update_source]");
        }
        if (conflicts.isEmpty() == false) {
            throw new IllegalArgumentException("Can't merge because of conflicts: " + conflicts);
        }
    }

}
