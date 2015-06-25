package org.atlasapi.content;

import static org.atlasapi.serialization.protobuf.ContentProtos.Column.AVAILABLE_CONTENT;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.BROADCASTS;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.CHILDREN;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.CHILD_UPDATED;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.CLIPS;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.DESC;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.GROUPS;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.IDENT;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.ITEM_SUMMARIES;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.KEYPHRASES;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.LINKS;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.LOCATIONS;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.PEOPLE;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.SECONDARY;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.SEGMENTS;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.SOURCE;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.TOPICS;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.TYPE;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.UPCOMING_CONTENT;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import com.google.protobuf.Descriptors;
import com.netflix.astyanax.model.Column;
import org.atlasapi.entity.Serializer;
import org.atlasapi.serialization.protobuf.CommonProtos.Reference;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.serialization.protobuf.ContentProtos.Content.Builder;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.EnumBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.model.ColumnList;

public class ProtobufContentMarshaller implements ContentMarshaller {
    private static final String UPCOMING_CONTENT_PREFIX = "UPCOMING_BROADCASTS";
    private static final String AVAILABLE_CONTENT_PREFIX = "AVAILABLE";
    private static final String ITEM_SUMMARY_PREFIX = "ITEM_SUMMARY";

    private final ListMultimap<ContentProtos.Column, FieldDescriptor> schema =
        Multimaps.index(
                ContentProtos.Content.getDescriptor().getFields(),
                new Function<FieldDescriptor, ContentProtos.Column>() {
                    @Override
                    public ContentProtos.Column apply(FieldDescriptor fd) {
                        return fd.getOptions().getExtension(ContentProtos.column);
                    }
                }
        );
    private final List<Entry<ContentProtos.Column, List<FieldDescriptor>>> schemaList =
        ImmutableList.copyOf(
            Maps.transformValues(schema.asMap(), 
                new Function<Collection<FieldDescriptor>, List<FieldDescriptor>>(){
        
                @Override
                public List<FieldDescriptor> apply(Collection<FieldDescriptor> input) {
                    return (List<FieldDescriptor>) input;
                }
            })
        .entrySet());
    
    private final Serializer<Content, ContentProtos.Content> serializer = new ContentSerializer();
    private final EnumBiMap<ContentProtos.Column, ContentColumn> columnLookup = EnumBiMap.create(
        ImmutableMap.<ContentProtos.Column, ContentColumn> builder()
            .put(TYPE, ContentColumn.TYPE)
            .put(SOURCE, ContentColumn.SOURCE)
            .put(IDENT, ContentColumn.IDENTIFICATION)
            .put(DESC, ContentColumn.DESCRIPTION)
            .put(BROADCASTS, ContentColumn.BROADCASTS)
            .put(LOCATIONS, ContentColumn.LOCATIONS)
            .put(CHILDREN, ContentColumn.CHILDREN)
            .put(CHILD_UPDATED, ContentColumn.CHILD_UPDATED)
            .put(SECONDARY, ContentColumn.SECONDARY)
            .put(PEOPLE, ContentColumn.PEOPLE)
            .put(CLIPS, ContentColumn.CLIPS)
            .put(KEYPHRASES, ContentColumn.KEYPHRASES)
            .put(LINKS, ContentColumn.LINKS)
            .put(TOPICS, ContentColumn.TOPICS)
            .put(GROUPS, ContentColumn.GROUPS)
            .put(SEGMENTS, ContentColumn.SEGMENTS)
            .put(UPCOMING_CONTENT, ContentColumn.UPCOMING_CONTENT)
            .put(AVAILABLE_CONTENT, ContentColumn.AVAILABLE_CONTENT)
            .put(ITEM_SUMMARIES, ContentColumn.ITEM_SUMMARIES)
        .build());

    @Override
    public void marshallInto(ColumnListMutation<String> mutation, Content content) {
        ContentProtos.Content proto = serializer.serialize(content);
        for (int i = 0; i < schemaList.size(); i++) {
            Entry<ContentProtos.Column, List<FieldDescriptor>> col = schemaList.get(i);
            if (isChildRefColumn(col.getKey())) {
                handleChildRefColumn(mutation, proto, Iterables.getOnlyElement(col.getValue()));
                continue;
            }
            if (isUpcomingContentColumn(col.getKey())) {
                handleUpcomingContentColumn(mutation, proto, Iterables.getOnlyElement(col.getValue()));
                continue;
            }

            if (ContentProtos.Column.AVAILABLE_CONTENT.equals(col.getKey())) {
                handleAvailableContentColumn(mutation, proto, Iterables.getOnlyElement(col.getValue()));
                continue;
            }

            if (ContentProtos.Column.ITEM_SUMMARIES.equals(col.getKey())) {
                handleItemSummariesColumn(mutation, proto, Iterables.getOnlyElement(col.getValue()));
                continue;
            }
            Builder builder = null;
            for (int j = 0; j < col.getValue().size(); j++) {
                FieldDescriptor fd = col.getValue().get(j);
                if (fd.isRepeated()) {
                    if (proto.getRepeatedFieldCount(fd) > 0) {
                        builder = getBuilder(builder);
                        for (int k = 0; k < proto.getRepeatedFieldCount(fd); k++) {
                            builder.addRepeatedField(fd, proto.getRepeatedField(fd, k));
                        }
                    }
                } else if (proto.hasField(fd)) {
                    builder = getBuilder(builder);
                    builder.setField(fd, proto.getField(fd));
                }
            }
            if (builder != null) {
                mutation.putColumn(String.valueOf(columnLookup.get(col.getKey())), builder.build().toByteArray());
            }
        }
    }

    private boolean isUpcomingContentColumn(ContentProtos.Column key) {
        return ContentProtos.Column.UPCOMING_CONTENT.equals(key);
    }

    private void handleChildRefColumn(ColumnListMutation<String> mutation,
                                      ContentProtos.Content msg,
                                      FieldDescriptor fd) {
        if (msg.getRepeatedFieldCount(fd) == 1) {
            Reference cr = (Reference) msg.getRepeatedField(fd, 0);
            ContentProtos.Content col = ContentProtos.Content.newBuilder()
                .addRepeatedField(fd, cr)
                .build();
            mutation.putColumn(String.valueOf(cr.getId()), col.toByteArray());
        }
    }

    private void handleUpcomingContentColumn(ColumnListMutation<String> mutation,
                                      ContentProtos.Content msg,
                                      FieldDescriptor fd) {
        if (msg.getRepeatedFieldCount(fd) == 1) {
            ContentProtos.ItemAndBroadcastRef uc = (ContentProtos.ItemAndBroadcastRef) msg.getRepeatedField(fd, 0);
            ContentProtos.Content col = ContentProtos.Content.newBuilder()
                    .addRepeatedField(fd, uc)
                    .build();
            mutation.putColumn(
                    String.format(
                            "%s:%s",
                            UPCOMING_CONTENT_PREFIX,
                            uc.getItem().getId()
                    ),
                    col.toByteArray()
            );
        }
    }

    private void handleAvailableContentColumn(ColumnListMutation<String> mutation,
                                             ContentProtos.Content msg,
                                             FieldDescriptor fd) {
        if (msg.getRepeatedFieldCount(fd) == 1) {
            ContentProtos.ItemAndLocationSummary uc = (ContentProtos.ItemAndLocationSummary) msg.getRepeatedField(fd, 0);
            ContentProtos.Content col = ContentProtos.Content.newBuilder()
                    .addRepeatedField(fd, uc)
                    .build();
            mutation.putColumn(
                    String.format(
                            "%s:%s",
                            AVAILABLE_CONTENT_PREFIX,
                            uc.getItem().getId()
                    ),
                    col.toByteArray()
            );
        }
    }

    private void handleItemSummariesColumn(ColumnListMutation<String> mutation,
                                           ContentProtos.Content msg,
                                           FieldDescriptor fd) {
        if (msg.getRepeatedFieldCount(fd) == 1) {
            ContentProtos.ItemSummary uc = (ContentProtos.ItemSummary) msg.getRepeatedField(fd, 0);
            ContentProtos.Content col = ContentProtos.Content.newBuilder()
                    .addRepeatedField(fd, uc)
                    .build();
            mutation.putColumn(
                    String.format(
                            "%s:%s",
                            ITEM_SUMMARY_PREFIX,
                            uc.getItemRef().getId()
                    ),
                    col.toByteArray()
            );
        }
    }

    private boolean isChildRefColumn(ContentProtos.Column key) {
        return ImmutableSet.of(ContentProtos.Column.CHILDREN, ContentProtos.Column.SECONDARY).contains(key);
    }

    private Builder getBuilder(Builder builder) {
        return builder == null ? ContentProtos.Content.newBuilder() : builder;
    }

    @Override
    public Content unmarshallCols(ColumnList<String> columns) {
        ContentProtos.Content.Builder builder = ContentProtos.Content.newBuilder();
        for (Column<String> column : columns) {
            try {
                builder.mergeFrom(column.getByteArrayValue());
            } catch (InvalidProtocolBufferException e) {
                throw Throwables.propagate(e);
            }
        }
        return serializer.deserialize(builder.build());
    }
    
}
