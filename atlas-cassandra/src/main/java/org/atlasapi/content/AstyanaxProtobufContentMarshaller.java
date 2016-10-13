package org.atlasapi.content;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.Serializer;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.serialization.protobuf.ContentProtos;

import com.metabroadcast.common.stream.MoreCollectors;

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
import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.content.ContentColumn.IDENTIFICATION;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.ACTIVELY_PUBLISHED;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.AVAILABLE_CONTENT;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.BROADCASTS;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.CHILDREN;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.CHILD_UPDATED;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.CLIPS;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.CONTAINER_REF;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.CONTAINER_SUMMARY;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.DESC;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.GENERIC_DESCRIPTION;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.GROUPS;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.IDENT;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.ITEM_SUMMARIES;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.KEYPHRASES;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.LINKS;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.LOCATIONS;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.PEOPLE;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.SECONDARY;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.SEGMENTS;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.SERIES_REF;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.SOURCE;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.TOPICS;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.TYPE;
import static org.atlasapi.serialization.protobuf.ContentProtos.Column.UPCOMING_CONTENT;

public class AstyanaxProtobufContentMarshaller
        implements ContentMarshaller<ColumnListMutation<String>,ColumnList<String>> {

    public static final Set<ContentColumn> REQUIRED_CONTENT_COLUMNS = ImmutableSet.of(
            ContentColumn.TYPE,
            ContentColumn.SOURCE,
            IDENTIFICATION
    );
    private static final String UPCOMING_CONTENT_PREFIX = "UPCOMING_BROADCASTS";
    private static final String AVAILABLE_CONTENT_PREFIX = "AVAILABLE";
    private static final String ITEM_SUMMARY_PREFIX = "ITEM_SUMMARY";
    private static final String BROADCAST_PREFIX = "BROADCAST";

    private final Serializer<Content, ContentProtos.Content> serializer;
    private final ListMultimap<ContentProtos.Column, Descriptors.FieldDescriptor> schema =
            Multimaps.index(
                    ContentProtos.Content.getDescriptor().getFields(),
                    fd -> fd.getOptions().getExtension(ContentProtos.column)
            );

    private final List<Map.Entry<ContentProtos.Column, List<Descriptors.FieldDescriptor>>>
            schemaList =
            ImmutableList.copyOf(
                    Maps.transformValues(
                            schema.asMap(),
                            input -> (List<Descriptors.FieldDescriptor>) input
                    )
                            .entrySet());

    private final EnumBiMap<ContentProtos.Column, ContentColumn> columnLookup = EnumBiMap.create(
            ImmutableMap.<ContentProtos.Column, ContentColumn>builder()
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
                    .put(ACTIVELY_PUBLISHED, ContentColumn.ACTIVELY_PUBLISHED)
                    .put(GENERIC_DESCRIPTION, ContentColumn.GENERIC_DESCRIPTION)
                    .put(CONTAINER_SUMMARY, ContentColumn.CONTAINER_SUMMARY)
                    .put(CONTAINER_REF, ContentColumn.CONTAINER_REF)
                    .put(SERIES_REF, ContentColumn.SERIES_REF)
                    .build());

    private AstyanaxProtobufContentMarshaller(
            Serializer<Content, ContentProtos.Content> serialiser
    ) {
        this.serializer = checkNotNull(serialiser);
    }

    public static AstyanaxProtobufContentMarshaller create(
            Serializer<Content, ContentProtos.Content> serialiser
    ) {
        return new AstyanaxProtobufContentMarshaller(serialiser);
    }

    public static String buildItemSummaryKey(Long id) {
        return String.format(
                "%s:%s",
                ITEM_SUMMARY_PREFIX,
                id
        );
    }

    public static String buildAvailableContentKey(Long id) {
        return String.format(
                "%s:%s",
                AVAILABLE_CONTENT_PREFIX,
                id
        );
    }

    public static String buildUpcomingContentKey(Long id) {
        return String.format(
                "%s:%s",
                UPCOMING_CONTENT_PREFIX,
                id
        );
    }

    private static String buildBroadcastKey(String id) {
        return String.format(
                "%s:%s",
                BROADCAST_PREFIX,
                id
        );
    }

    @Override
    public void marshallInto(
            Id id,
            ColumnListMutation<String> mutation,
            Content content,
            Optional<Content> previousContent,
            Boolean setEmptyRepeatedFieldsToNull
    ) {
        ContentProtos.Content proto = serializer.serialize(content);
        for (int i = 0; i < schemaList.size(); i++) {
            Map.Entry<ContentProtos.Column, List<Descriptors.FieldDescriptor>> col =
                    schemaList.get(i);

            if (isChildRefColumn(col.getKey())) {
                handleChildRefColumn(mutation, id, proto, Iterables.getOnlyElement(col.getValue()));
                continue;
            }

            if (ContentProtos.Column.UPCOMING_CONTENT.equals(col.getKey())) {
                handleUpcomingContentColumn(
                        mutation,
                        id,
                        proto,
                        Iterables.getOnlyElement(col.getValue())
                );
                continue;
            }

            if (ContentProtos.Column.AVAILABLE_CONTENT.equals(col.getKey())) {
                handleAvailableContentColumn(
                        mutation,
                        id,
                        proto,
                        Iterables.getOnlyElement(col.getValue())
                );
                continue;
            }

            if (ContentProtos.Column.ITEM_SUMMARIES.equals(col.getKey())) {
                handleItemSummariesColumn(
                        mutation,
                        id,
                        proto,
                        Iterables.getOnlyElement(col.getValue())
                );
                continue;
            }

            if (ContentProtos.Column.BROADCASTS.equals(col.getKey())) {
                handleBroadcastColumn(
                        mutation,
                        id,
                        proto,
                        getStaleBroadcastSourceIds(content, previousContent),
                        Iterables.getOnlyElement(col.getValue())
                );
                continue;
            }

            ContentProtos.Content.Builder builder = null;
            for (int j = 0; j < col.getValue().size(); j++) {
                Descriptors.FieldDescriptor fd = col.getValue().get(j);
                if (fd.isRepeated()) {
                    if (setEmptyRepeatedFieldsToNull) {
                        builder = getBuilder(builder);
                    }
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
                addColumnToBatch(
                        mutation,
                        id,
                        String.valueOf(columnLookup.get(col.getKey())),
                        builder.build().toByteArray()
                );
            }
        }
    }

    @Override
    public Content unmarshallCols(ColumnList<String> columns) {
        ContentProtos.Content.Builder builder = ContentProtos.Content.newBuilder();
        for (byte[] column : toByteArrayValues(columns)) {
            try {
                builder.mergeFrom(column);
            } catch (InvalidProtocolBufferException e) {
                throw Throwables.propagate(e);
            }
        }
        return serializer.deserialize(builder.build());
    }

    private void addColumnToBatch(ColumnListMutation<String> mutation, Id id, String column,
            byte[] value) {
        mutation.putColumn(column, value);
    }

    private void removeColumn(ColumnListMutation<String> mutation, String column) {
        mutation.deleteColumn(column);
    }

    private Iterable<byte[]> toByteArrayValues(ColumnList<String> columns) {
        return Iterables.transform(
                columns,
                new Function<Column<String>, byte[]>() {

                    @Nullable
                    @Override
                    public byte[] apply(Column<String> column) {
                        return column.getByteArrayValue();
                    }
                }
        );
    }

    private void handleChildRefColumn(
            ColumnListMutation<String> mutation,
            Id id,
            ContentProtos.Content msg,
            Descriptors.FieldDescriptor fd
    ) {
        if (msg.getRepeatedFieldCount(fd) == 1) {
            CommonProtos.Reference cr = (CommonProtos.Reference) msg.getRepeatedField(fd, 0);
            ContentProtos.Content col = ContentProtos.Content.newBuilder()
                    .addRepeatedField(fd, cr)
                    .build();
            addColumnToBatch(
                    mutation,
                    id,
                    String.valueOf(cr.getId()),
                    col.toByteArray()
            );
        }
    }

    private void handleUpcomingContentColumn(
            ColumnListMutation<String> mutation,
            Id id,
            ContentProtos.Content msg,
            Descriptors.FieldDescriptor fd
    ) {
        if (msg.getRepeatedFieldCount(fd) == 1) {
            ContentProtos.ItemAndBroadcastRef uc =
                    (ContentProtos.ItemAndBroadcastRef) msg.getRepeatedField(fd, 0);
            ContentProtos.Content col = ContentProtos.Content.newBuilder()
                    .addRepeatedField(fd, uc)
                    .build();
            addColumnToBatch(
                    mutation,
                    id,
                    buildUpcomingContentKey(uc.getItem().getId()),
                    col.toByteArray()
            );
        }
    }

    private void handleAvailableContentColumn(
            ColumnListMutation<String> mutation,
            Id id,
            ContentProtos.Content msg,
            Descriptors.FieldDescriptor fd
    ) {
        if (msg.getRepeatedFieldCount(fd) == 1) {
            ContentProtos.ItemAndLocationSummary uc =
                    (ContentProtos.ItemAndLocationSummary) msg.getRepeatedField(fd, 0);
            ContentProtos.Content col = ContentProtos.Content.newBuilder()
                    .addRepeatedField(fd, uc)
                    .build();
            addColumnToBatch(
                    mutation,
                    id,
                    buildAvailableContentKey(uc.getItem().getId()),
                    col.toByteArray()
            );
        }
    }

    private void handleItemSummariesColumn(
            ColumnListMutation<String> mutation,
            Id id,
            ContentProtos.Content msg,
            Descriptors.FieldDescriptor fd
    ) {
        if (msg.getRepeatedFieldCount(fd) == 1) {
            ContentProtos.ItemSummary uc = (ContentProtos.ItemSummary) msg.getRepeatedField(fd, 0);
            ContentProtos.Content col = ContentProtos.Content.newBuilder()
                    .addRepeatedField(fd, uc)
                    .build();
            addColumnToBatch(
                    mutation,
                    id,
                    buildItemSummaryKey(uc.getItemRef().getId()),
                    col.toByteArray()
            );
        }
    }

    private void handleBroadcastColumn(
            ColumnListMutation<String> mutation,
            Id id,
            ContentProtos.Content msg,
            Iterable<String> staleBroadcastSourceIds,
            Descriptors.FieldDescriptor fd
    ) {
        int broadcastCount = msg.getRepeatedFieldCount(fd);
        for (int i = 0; i < broadcastCount; i++) {
            ContentProtos.Broadcast uc = (ContentProtos.Broadcast) msg.getRepeatedField(fd, i);
            ContentProtos.Content col = ContentProtos.Content.newBuilder()
                    .addRepeatedField(fd, uc)
                    .build();
            addColumnToBatch(
                    mutation,
                    id,
                    buildBroadcastKey(uc.getSourceId()),
                    col.toByteArray()
            );
        }

        for (String staleBroadcastSourceId : staleBroadcastSourceIds) {
            removeColumn(mutation, buildBroadcastKey(staleBroadcastSourceId));
        }
    }

    private boolean isChildRefColumn(ContentProtos.Column key) {
        return ImmutableSet.of(ContentProtos.Column.CHILDREN, ContentProtos.Column.SECONDARY)
                .contains(key);
    }

    private ContentProtos.Content.Builder getBuilder(
            @Nullable ContentProtos.Content.Builder builder
    ) {
        return builder == null ? ContentProtos.Content.newBuilder() : builder;
    }

    private ImmutableSet<String> getStaleBroadcastSourceIds(
            Content content,
            Optional<Content> previousContent
    ) {
        if (!previousContent.isPresent()) {
            return ImmutableSet.of();
        }

        Content previous = previousContent.get();

        if (!(previous instanceof Item)) {
            return ImmutableSet.of();
        }

        ImmutableSet<String> previousSourceIds = ((Item) previous).getBroadcasts()
                .stream()
                .map(Broadcast::getSourceId)
                .collect(MoreCollectors.toImmutableSet());

        if (!(content instanceof Item)) {
            return previousSourceIds;
        }

        ImmutableSet<String> sourceIds = ((Item) content).getBroadcasts()
                .stream()
                .map(Broadcast::getSourceId)
                .collect(MoreCollectors.toImmutableSet());

        return Sets.difference(previousSourceIds, sourceIds)
                .immutableCopy();
    }
}
