package org.atlasapi.content;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.AliasIndex;
import org.atlasapi.entity.CassandraPersistenceException;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.util.ImmutableCollectors;

import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Clock;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.content.ContentColumn.DESCRIPTION;
import static org.atlasapi.content.ContentColumn.IDENTIFICATION;
import static org.atlasapi.content.ContentColumn.SOURCE;
import static org.atlasapi.content.ContentColumn.TYPE;

public class DatastaxCassandraContentStore extends AbstractContentStore {

    private static final String CONTENT_TABLE = "content";
    private static final String PRIMARY_KEY_COLUMN = "key";
    private static final String CLUSTERING_KEY_COLUMN = "column1";

    private final AliasIndex<Content> aliasIndex;
    private final Session session;
    private final ConsistencyLevel writeConsistency;
    private final ConsistencyLevel readConsistency;
    private final ContentMarshaller<BatchStatement, Iterable<Row>> marshaller;

    private final PreparedStatement summarizeSelect;
    private final PreparedStatement idSelect;
    private final PreparedStatement contentDelete;

    public DatastaxCassandraContentStore(
            ContentHasher hasher,
            IdGenerator idGenerator,
            MessageSender<ResourceUpdatedMessage> sender,
            Clock clock,
            Session session,
            ConsistencyLevel writeConsistency,
            ConsistencyLevel readConsistency,
            AliasIndex<Content> aliasIndex
    ) {
        super(hasher, idGenerator, sender, clock);
        this.aliasIndex = checkNotNull(aliasIndex);
        this.session = checkNotNull(session);
        this.marshaller = new DatastaxProtobufContentMarshaller(new ContentSerializer(new ContentSerializationVisitor(
                this)), session);
        this.writeConsistency = checkNotNull(writeConsistency);
        this.readConsistency = checkNotNull(readConsistency);

        this.summarizeSelect = session.prepare(select().all().from(CONTENT_TABLE)
                .where(eq(PRIMARY_KEY_COLUMN, bindMarker("key")))
                .and(
                        in(
                                CLUSTERING_KEY_COLUMN,
                                TYPE.toString(),
                                SOURCE.toString(),
                                IDENTIFICATION.toString(),
                                DESCRIPTION.toString()
                        )
                ));
        this.summarizeSelect.setConsistencyLevel(readConsistency);

        this.idSelect = session.prepare(select().all()
                .from(CONTENT_TABLE)
                .where(
                        eq(PRIMARY_KEY_COLUMN, bindMarker())
                ));
        this.idSelect.setConsistencyLevel(readConsistency);

        this.contentDelete = session.prepare(delete().all()
                .from(CONTENT_TABLE)
                .where(eq(PRIMARY_KEY_COLUMN, bindMarker("key")))
                .and(eq(CLUSTERING_KEY_COLUMN, bindMarker("clustering"))));
    }

    @Override
    protected Optional<Content> resolvePrevious(Optional<Id> id, Publisher source,
            Set<Alias> aliases) {
        try {
            if (id.isPresent()) {
                return Futures.get(
                        resolveIds(ImmutableSet.of(id.get())),
                        1, TimeUnit.MINUTES,
                        IOException.class
                ).getResources().first();
            } else {
                Set<Long> ids = aliasIndex.readAliases(source, aliases);
                Long aliasId = Iterables.getFirst(ids, null);
                if (aliasId != null) {
                    return Futures.get(
                            resolveIds(ImmutableSet.of(Id.valueOf(aliasId))),
                            1, TimeUnit.MINUTES,
                            IOException.class
                    ).getResources().first();
                }
                return Optional.absent();
            }
        } catch (ConnectionException | IOException e) {
            throw Throwables.propagate(e);
        } catch (CorruptContentException e) {
            log.error("Previously written content is corrupt", e);
            return Optional.absent();
        }
    }

    @Override
    protected void doWriteContent(Content content, Content previous) {
        try {
            BatchStatement batch = new BatchStatement();
            batch.setConsistencyLevel(writeConsistency);
            marshaller.marshallInto(content.getId(), batch, content, true);
            aliasIndex.mutateAliasesAndExecute(content, previous);
            session.execute(batch);
        } catch (Exception e) {
            throw new CassandraPersistenceException(content.toString(), e);
        }
    }

    @Override
    protected ContainerSummary summarize(ContainerRef primary) {
        ResultSet rows = session.execute(summarizeSelect.bind()
                .setLong("key", primary.getId().longValue()));
        if (rows.isExhausted()) {
            return null;
        }
        Content content = marshaller.unmarshallCols(rows);
        if (!(content instanceof Container)) {
            throw new IllegalStateException(String.format(
                    "Content for parent %s not Container",
                    primary.getId()
            ));
        }
        return ((Container) content).toSummary();
    }

    @Override
    protected void writeSecondaryContainerRef(BrandRef primary, SeriesRef seriesRef,
            Boolean activelyPublished) {
        try {
            if (!activelyPublished) {
                BatchStatement batch = new BatchStatement();
                batch.setConsistencyLevel(writeConsistency);
                batch.add(removeContentRef(primary, seriesRef));
                session.execute(batch);
                return;
            }
            Brand container = new Brand();
            container.setSeriesRefs(ImmutableList.of(seriesRef));
            container.setThisOrChildLastUpdated(seriesRef.getUpdated());

            BatchStatement batch = new BatchStatement();
            batch.setConsistencyLevel(writeConsistency);
            marshaller.marshallInto(primary.getId(), batch, container, false);
            session.execute(batch);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected void writeItemRefs(Item item) {
        try {
            ensureId(item);
            if (!item.isActivelyPublished() || (item.isGenericDescription() != null
                    && item.isGenericDescription())) {
                removeItemRefsFromContainers(item);
                return;
            }
            ContainerRef containerRef = item.getContainerRef();
            ImmutableMap<ItemRef, Iterable<BroadcastRef>> upcomingBroadcasts = ImmutableMap.of(
                    item.toRef(), item.getUpcomingBroadcastRefs()
            );
            ImmutableMap<ItemRef, Iterable<LocationSummary>> availableLocations = ImmutableMap.of(
                    item.toRef(), item.getAvailableLocations()
            );

            BatchStatement batch = new BatchStatement();
            batch.setConsistencyLevel(writeConsistency);
            if (containerRef != null) {
                Id containerId = containerRef.getId();
                Container container = new Brand();
                container.setItemRefs(ImmutableList.of(item.toRef()));
                container.setThisOrChildLastUpdated(item.toRef().getUpdated());
                container.setUpcomingContent(upcomingBroadcasts);
                container.setAvailableContent(availableLocations);
                if (!(item instanceof Episode) || ((Episode) item).getSeriesRef() == null) {
                    container.setItemSummaries(ImmutableList.of(item.toSummary()));
                }
                marshaller.marshallInto(containerId, batch, container, false);
            }

            if (item instanceof Episode && ((Episode) item).getSeriesRef() != null) {
                Episode episode = (Episode) item;
                Id containerId = episode.getSeriesRef().getId();
                Container container = new Series();
                container.setItemRefs(ImmutableList.of(item.toRef()));
                container.setThisOrChildLastUpdated(item.toRef().getUpdated());
                container.setUpcomingContent(upcomingBroadcasts);
                container.setAvailableContent(availableLocations);
                container.setItemSummaries(ImmutableList.of(episode.toSummary()));
                marshaller.marshallInto(containerId, batch, container, false);
            }
            session.execute(batch);

        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected void doWriteBroadcast(
            ItemRef itemRef,
            Optional<ContainerRef> containerRef,
            Optional<SeriesRef> seriesRef,
            Broadcast broadcast
    ) {
        Item item = new Item();
        item.setId(itemRef.getId());
        item.setThisOrChildLastUpdated(itemRef.getUpdated());
        item.addBroadcast(broadcast);
        item.setPublisher(itemRef.getSource());
        BatchStatement batch = new BatchStatement();
        batch.setConsistencyLevel(writeConsistency);
        marshaller.marshallInto(item.getId(), batch, item, false);

        if (!broadcast.isActivelyPublished() || !item.getUpcomingBroadcastRefs()
                .iterator()
                .hasNext()) {
            session.execute(batch);
            return;
        }
        ImmutableMap<ItemRef, Iterable<BroadcastRef>> upcomingBroadcasts = ImmutableMap.of(
                itemRef, item.getUpcomingBroadcastRefs()
        );
        if (containerRef.isPresent()) {
            Id containerId = containerRef.get().getId();
            Container container = new Brand();
            container.setThisOrChildLastUpdated(itemRef.getUpdated());
            container.setUpcomingContent(upcomingBroadcasts);
            marshaller.marshallInto(containerId, batch, container, false);
        }

        if (seriesRef.isPresent()) {
            Id containerId = seriesRef.get().getId();
            Container container = new Series();
            container.setItemRefs(ImmutableList.of(itemRef));
            container.setThisOrChildLastUpdated(itemRef.getUpdated());
            container.setUpcomingContent(upcomingBroadcasts);
            marshaller.marshallInto(containerId, batch, container, false);
        }

        session.execute(batch);
    }

    @Override
    protected void writeContainerSummary(ContainerSummary summary, Iterable<ItemRef> items) {
        BatchStatement batch = new BatchStatement();
        batch.setConsistencyLevel(writeConsistency);
        for (ItemRef itemRef : items) {
            Item item = itemFromRef(itemRef);
            item.setContainerSummary(summary);
            marshaller.marshallInto(itemRef.getId(), batch, item, false);

        }

        session.execute(batch);

    }

    @Override
    protected void removeAllReferencesToItem(ContainerRef containerRef, ItemRef itemRef) {
        BatchStatement batchStatement = new BatchStatement();
        batchStatement.setConsistencyLevel(writeConsistency);

        for (Statement statement : removeItemReferencesStatments(containerRef, itemRef)) {
            batchStatement.add(statement);
        }

        session.execute(batchStatement);
    }

    @Override
    public OptionalMap<Alias, Content> resolveAliases(Iterable<Alias> aliases, Publisher source) {
        try {
            Set<Alias> uniqueAliases = ImmutableSet.copyOf(aliases);
            Set<Id> ids = aliasIndex.readAliases(source, uniqueAliases)
                    .stream()
                    .map(Id::valueOf)
                    .collect(Collectors.toSet());
            if (ids.isEmpty()) {
                return ImmutableOptionalMap.of();
            }
            Iterable<Content> resources = resolveIds(ids).get(1, TimeUnit.MINUTES).getResources();

            ImmutableMap.Builder<Alias, Optional<Content>> aliasMap = ImmutableMap.builder();
            for (Content content : resources) {
                Set<Alias> contentAliases = content.getAliases()
                        .stream()
                        .filter(uniqueAliases::contains)
                        .collect(Collectors.toSet());
                for (Alias contentAlias : contentAliases) {
                    aliasMap.put(contentAlias, Optional.of(content));
                }

            }
            return ImmutableOptionalMap.copyOf(aliasMap.build());
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ListenableFuture<Resolved<Content>> resolveIds(Iterable<Id> ids) {
        ListenableFuture<List<Row>> resultsFuture = Futures.transform(
                Futures.allAsList(
                        StreamSupport.stream(ids.spliterator(), false)
                                .map(Id::longValue)
                                .unordered()
                                .distinct()
                                .map(idSelect::bind)
                                .map(session::executeAsync)
                                .map(rsFuture -> Futures.transform(
                                        rsFuture,
                                        (Function<ResultSet, List<Row>>) input ->
                                                input != null ? input.all() : Lists.newArrayList()
                                ))
                                .collect(Collectors.toList())),
                (Function<List<List<Row>>, List<Row>>) input -> input.stream()
                        .flatMap(Collection::stream)
                        .collect(ImmutableCollectors.toList())
        );

        return Futures.transform(
                resultsFuture,
                (Iterable<Row> input) -> {
                    return Resolved.valueOf(
                            StreamSupport.stream(input.spliterator(), false)
                                    .collect(Collectors.groupingBy(row -> row.getLong(
                                            PRIMARY_KEY_COLUMN)))
                                    .values()
                                    .stream()
                                    .map(cs -> {
                                        verifyRequiredColumns(cs);
                                        return marshaller.unmarshallCols(cs);
                                    })
                                    .collect(Collectors.toList()));
                }
        );
    }

    private void verifyRequiredColumns(List<Row> rows) {
        Long id = null;
        for (ContentColumn requiredColumn : ProtobufContentMarshaller.REQUIRED_CONTENT_COLUMNS) {
            boolean columnFound = false;
            for (Row row : rows) {
                id = row.getLong(PRIMARY_KEY_COLUMN);
                if (requiredColumn.toString().equals(row.getString(CLUSTERING_KEY_COLUMN))) {
                    columnFound = true;
                    break;
                }

            }
            if (!columnFound) {
                throw new CorruptContentException(
                        String.format(
                                "Missing required column '%s' in row with ID %s in CassandraContentStore.",
                                requiredColumn.toString(),
                                id
                        )
                );
            }

        }
    }

    private void removeItemRefsFromContainers(Item item) throws ConnectionException {
        BatchStatement batch = new BatchStatement();
        batch.setConsistencyLevel(writeConsistency);

        if (item.getContainerRef() != null) {
            for (Statement statement : removeItemReferencesStatments(
                    item.getContainerRef(),
                    item.toRef()
            )) {
                batch.add(statement);
            }

        }
        if (item instanceof Episode && ((Episode) item).getSeriesRef() != null) {
            Episode episode = (Episode) item;
            for (Statement statement : removeItemReferencesStatments(
                    episode.getSeriesRef(),
                    episode.toRef()
            )) {
                batch.add(statement);
            }
        }
        session.execute(batch);
    }

    private Iterable<Statement> removeItemReferencesStatments(ContainerRef containerRef,
            ItemRef itemRef) {
        return ImmutableSet.<Statement>builder()
                .add(removeContentRef(containerRef, itemRef))
                .add(removeItemSummaries(containerRef, itemRef))
                .add(removeAvailableContent(containerRef, itemRef))
                .add(removeUpcomingContent(containerRef, itemRef))
                .build();

    }

    private Statement removeContentRef(ContainerRef containerRef, ContentRef contentRef) {
        Long rowId = containerRef.getId().longValue();
        String columnId = contentRef.getId().toString();
        return contentDelete.bind().setLong("key", rowId).setString("clustering", columnId);
    }

    private Statement removeItemSummaries(ContainerRef containerRef, ItemRef itemRef) {
        Long rowId = containerRef.getId().longValue();
        String columnId = ProtobufContentMarshaller.buildItemSummaryKey(itemRef.getId()
                .longValue());
        return contentDelete.bind().setLong("key", rowId).setString("clustering", columnId);
    }

    private Statement removeAvailableContent(ContainerRef containerRef, ItemRef itemRef) {
        Long rowId = containerRef.getId().longValue();
        String columnId = ProtobufContentMarshaller.buildAvailableContentKey(itemRef.getId()
                .longValue());
        return contentDelete.bind().setLong("key", rowId).setString("clustering", columnId);
    }

    private Statement removeUpcomingContent(ContainerRef brancontainerRefRef, ItemRef itemRef) {
        Long rowId = brancontainerRefRef.getId().longValue();
        String columnId = ProtobufContentMarshaller.buildUpcomingContentKey(itemRef.getId()
                .longValue());
        return contentDelete.bind().setLong("key", rowId).setString("clustering", columnId);
    }

    private Item itemFromRef(ItemRef itemRef) {
        Item item;
        if (itemRef instanceof EpisodeRef) {
            item = new Episode();
        } else {
            item = new Item();
        }
        item.setId(itemRef.getId());
        item.setThisOrChildLastUpdated(itemRef.getUpdated());
        item.setPublisher(itemRef.getSource());
        return item;
    }
}
