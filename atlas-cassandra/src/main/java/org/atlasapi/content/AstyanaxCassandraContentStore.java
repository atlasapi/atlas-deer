package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.content.ContentColumn.DESCRIPTION;
import static org.atlasapi.content.ContentColumn.IDENTIFICATION;
import static org.atlasapi.content.ContentColumn.SOURCE;
import static org.atlasapi.content.ContentColumn.TYPE;

import java.util.List;
import java.util.Map;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public final class AstyanaxCassandraContentStore extends AbstractContentStore {
    
    protected final Logger log = LoggerFactory.getLogger(getClass());

    public static final Builder builder(AstyanaxContext<Keyspace> context, 
            String name, ContentHasher hasher, MessageSender<ResourceUpdatedMessage> sender, IdGenerator idGenerator) {
        return new Builder(context, name, hasher, sender, idGenerator);
    }

    public static final class Builder {

        private final AstyanaxContext<Keyspace> context;
        private final String name;
        private final ContentHasher hasher;
        private final MessageSender<ResourceUpdatedMessage> sender;
        private final IdGenerator idGenerator;

        private ConsistencyLevel readCl = ConsistencyLevel.CL_QUORUM;
        private ConsistencyLevel writeCl = ConsistencyLevel.CL_QUORUM;
        private Clock clock = new SystemClock();

        public Builder(AstyanaxContext<Keyspace> context, String name,
                       ContentHasher hasher, MessageSender<ResourceUpdatedMessage> sender, IdGenerator idGenerator) {
            this.context = context;
            this.name = name;
            this.hasher = hasher;
            this.sender = sender;
            this.idGenerator = idGenerator;
        }

        public Builder withReadConsistency(ConsistencyLevel readCl) {
            this.readCl = readCl;
            return this;
        }

        public Builder withWriteConsistency(ConsistencyLevel writeCl) {
            this.writeCl = writeCl;
            return this;
        }

        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public AstyanaxCassandraContentStore build() {
            return new AstyanaxCassandraContentStore(context, name, readCl, writeCl,
                hasher, idGenerator, sender, clock);
        }

    }


    private final Keyspace keyspace;
    private final ConsistencyLevel readConsistency;
    private final ConsistencyLevel writeConsistency;
    private final ColumnFamily<Long, String> mainCf;
    private final AliasIndex<Content> aliasIndex;

    private final ContentMarshaller<ColumnListMutation<String>,ColumnList<String>> marshaller = new AstyanaxProtobufContentMarshaller(new ContentSerializer(new ContentSerializationVisitor(this)));
    private final Function<Map.Entry<Long, ColumnList<String>>, Content> rowToContent =
            input -> {
                if (!input.getValue().isEmpty()) {
                    verifyRequiredColumns(input.getKey(), input.getValue());
                    return marshaller.unmarshallCols(input.getValue());
                }
                return null;
            };

    private void verifyRequiredColumns(Long id, ColumnList<String> columns) {
        for (ContentColumn requiredColumn : ProtobufContentMarshaller.REQUIRED_CONTENT_COLUMNS) {
            if(columns.getColumnByName(requiredColumn.toString()) == null) {
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

    private final Function<Map<Long, ColumnList<String>>, Resolved<Content>> toResolvedContent =
            rows -> Resolved.valueOf(FluentIterable.from(rows.entrySet()).transform(rowToContent).filter(Predicates.notNull()));

    public AstyanaxCassandraContentStore(AstyanaxContext<Keyspace> context,
        String cfName, ConsistencyLevel readConsistency, ConsistencyLevel writeConsistency, 
        ContentHasher hasher, IdGenerator idGenerator, MessageSender<ResourceUpdatedMessage> sender, Clock clock) {
        super(hasher, idGenerator, sender, clock);
        this.keyspace = checkNotNull(context.getClient());
        this.readConsistency = checkNotNull(readConsistency);
        this.writeConsistency = checkNotNull(writeConsistency);
        this.mainCf = ColumnFamily.newColumnFamily(checkNotNull(cfName),
            LongSerializer.get(), StringSerializer.get());
        this.aliasIndex = AliasIndex.create(keyspace,cfName+"_aliases");
    }
    
    @Override
    public ListenableFuture<Resolved<Content>> resolveIds(Iterable<Id> ids) {
        try {
            Iterable<Long> longIds = Iterables.transform(ids, Id.toLongValue());
            return Futures.transform(resolveLongs(longIds), toResolvedContent);
        } catch (Exception e) {
            throw new CassandraPersistenceException(Joiner.on(", ").join(ids), e);
        }
    }

    private ListenableFuture<Map<Long, ColumnList<String>>> resolveLongs(Iterable<Long> longIds) throws ConnectionException {
        return Futures.transform(Futures.allAsList(StreamSupport.stream(longIds.spliterator(), false)
                .map(id -> {
                    try {
                        return Futures.transform(
                                keyspace
                                        .prepareQuery(mainCf)
                                        .setConsistencyLevel(readConsistency)
                                        .getKey(id)
                                        .executeAsync(),
                                (Function<OperationResult<ColumnList<String>>, KeyValue<Long, ColumnList<String>>>)
                                        input -> new KeyValue<>(id, input.getResult()));
                    } catch (ConnectionException e) {
                        throw new CassandraPersistenceException(id.toString(), e);
                    }
                }).collect(Collectors.toList())),
                (Function<List<KeyValue<Long, ColumnList<String>>>, Map<Long, ColumnList<String>>>)
                        input -> input.stream().collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue)));
    }
    
    @Override
    public OptionalMap<Alias, Content> resolveAliases(Iterable<Alias> aliases, Publisher source) {
        try {
            Set<Alias> uniqueAliases = ImmutableSet.copyOf(aliases);
            Set<Long> ids = aliasIndex.readAliases(source, uniqueAliases);
            if (ids.isEmpty()) {
                return ImmutableOptionalMap.of();
            }
            // TODO: move timeout to config
            Map<Long, ColumnList<String>> resolved = resolveLongs(ids).get(10, TimeUnit.SECONDS);
            Iterable<Content> contents = resolved.entrySet()
                    .stream()
                    .map(rowToContent::apply)
                    .collect(Collectors.toList());
            ImmutableMap.Builder<Alias, com.google.common.base.Optional<Content>> aliasMap = ImmutableMap.builder();
            for (Content content : contents) {
                content.getAliases()
                        .stream()
                        .filter(uniqueAliases::contains)
                        .forEach(alias -> aliasMap.put(alias, Optional.of(content)));
            }
            return ImmutableOptionalMap.copyOf(aliasMap.build());
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected void doWriteContent(Content content, Content previous) {
        try {
            long id = content.getId().longValue();
            MutationBatch batch = keyspace.prepareMutationBatch();
            batch.setConsistencyLevel(writeConsistency);
            marshaller.marshallInto(content.getId(), batch.withRow(mainCf, id), content, true);
            batch.mergeShallow(aliasIndex.mutateAliases(content, previous));
            batch.execute();
            log.trace("Written content id " + id);
        } catch (Exception e) {
            throw new CassandraPersistenceException(content.toString(), e);
        }
    }
    
    @Override
    protected Optional<Content> resolvePrevious(Optional<Id> id, Publisher source, Set<Alias> aliases) {
        try {
            if (id.isPresent()) {
                return Optional.fromNullable(resolve(id.get().longValue(), null));
            } else {
                    Set<Long> ids = aliasIndex.readAliases(source, aliases);
                    Long aliasId = Iterables.getFirst(ids, null);
                    if (aliasId != null) {
                        return Optional.fromNullable(resolve(aliasId, null));
                    }
                return Optional.absent();
            }
        } catch (ConnectionException e) {
            throw Throwables.propagate(e);
        } catch (CorruptContentException e) {
            log.error("Previously written content is corrupt", e);
            return Optional.absent();
        }
    }

    private Content resolve(long longId, Set<ContentColumn> colNames) {
        try {
            RowQuery<Long, String> query = keyspace.prepareQuery(mainCf)
                .getKey(longId);
            if (colNames != null && colNames.size() > 0) {
                query = query.withColumnSlice(Collections2.transform(colNames, Functions.toStringFunction()));
            }
            ColumnList<String> cols = query.execute().getResult();
            if(cols.isEmpty()) {
                return null;
            }
            verifyRequiredColumns(longId, cols);
            return marshaller.unmarshallCols(cols);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected ContainerSummary summarize(ContainerRef id) {
        Content resolved = resolve(id.getId().longValue(),
            ImmutableSet.of(TYPE, SOURCE, IDENTIFICATION, DESCRIPTION));
        if (resolved instanceof Container) {
            return summarize((Container)resolved);
        } else  if (resolved == null) {
            return null;
        } else {
            throw new IllegalStateException(String.format("Content for parent %s not Container", id));
        }
    }

    private ContainerSummary summarize(Container container) {
        ContainerSummary summary = null;
        if (container != null) {
            summary = container.accept(new ContainerVisitor<ContainerSummary>() {

                @Override
                public ContainerSummary visit(Brand brand) {
                    return new ContainerSummary(
                            EntityType.from(brand).name(),
                            brand.getTitle(),
                            brand.getDescription(),
                            null
                    );
                }

                @Override
                public ContainerSummary visit(Series series) {
                    return new ContainerSummary(
                            EntityType.from(series).name(),
                            series.getTitle(),
                            series.getDescription(),
                            series.getSeriesNumber()
                    );
                }
                
            });
        }
        return summary;
    }

    @Override
    protected void writeSecondaryContainerRef(BrandRef primary, SeriesRef seriesRef, Boolean activelyPublished) {
        try {
            if(!activelyPublished) {
                MutationBatch batch = keyspace.prepareMutationBatch();
                batch.setConsistencyLevel(writeConsistency);
                removeContentRef(primary, seriesRef, batch);
                batch.execute();
                return;
            }
            Long rowId = primary.getId().longValue();
            Brand container = new Brand();
            container.setSeriesRefs(ImmutableList.of(seriesRef));
            container.setThisOrChildLastUpdated(seriesRef.getUpdated());
            
            MutationBatch batch = keyspace.prepareMutationBatch();
            batch.setConsistencyLevel(writeConsistency);
            ColumnListMutation<String> mutation = batch.withRow(mainCf, rowId);
            marshaller.marshallInto(primary.getId(), mutation, container, false);
            batch.execute();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private void removeContentRef(ContainerRef containerRef, ContentRef contentRef, MutationBatch batch)  {
        Long rowId = containerRef.getId().longValue();
        String columnId = contentRef.getId().toString();
        ColumnListMutation<String> mutation = batch.withRow(mainCf, rowId);
        mutation.deleteColumn(columnId);
    }

    private void removeItemSummaries(ContainerRef containerRef, ItemRef itemRef, MutationBatch batch)  {
        Long rowId = containerRef.getId().longValue();
        ColumnListMutation<String> mutation = batch.withRow(mainCf, rowId);
        mutation.deleteColumn(ProtobufContentMarshaller.buildItemSummaryKey(itemRef.getId().longValue()));
    }

    private void removeAvailableContent(ContainerRef containerRef, ItemRef itemRef, MutationBatch batch)  {
        Long rowId = containerRef.getId().longValue();
        ColumnListMutation<String> mutation = batch.withRow(mainCf, rowId);
        mutation.deleteColumn(ProtobufContentMarshaller.buildAvailableContentKey(itemRef.getId().longValue()));
    }

    private void removeUpcomingContent(ContainerRef brancontainerRefRef, ItemRef itemRef, MutationBatch batch) {
        Long rowId = brancontainerRefRef.getId().longValue();
        ColumnListMutation<String> mutation = batch.withRow(mainCf, rowId);
        mutation.deleteColumn(ProtobufContentMarshaller.buildUpcomingContentKey(itemRef.getId().longValue()));
    }


    @Override
    protected void writeItemRefs(
            Item item
    ) {
        try {
            ensureId(item);
            if(!item.isActivelyPublished() || (item.isGenericDescription() != null && item.isGenericDescription())) {
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

            MutationBatch batch = keyspace.prepareMutationBatch();
            batch.setConsistencyLevel(writeConsistency);
            if (containerRef != null) {
                Long rowId = containerRef.getId().longValue();
                Container container = new Brand();
                container.setItemRefs(ImmutableList.of(item.toRef()));
                container.setThisOrChildLastUpdated(item.toRef().getUpdated());
                container.setUpcomingContent(upcomingBroadcasts);
                container.setAvailableContent(availableLocations);
                if (!(item instanceof Episode) || ((Episode) item).getSeriesRef() == null) {
                    container.setItemSummaries(ImmutableList.of(item.toSummary()));
                }

                ColumnListMutation<String> mutation = batch.withRow(mainCf, rowId);
                marshaller.marshallInto(containerRef.getId(), mutation, container, false);
            }

            if (item instanceof Episode && ((Episode) item).getSeriesRef() != null) {
                Episode episode = (Episode) item;
                Long rowId = episode.getSeriesRef().getId().longValue();
                Container container = new Series();
                container.setItemRefs(ImmutableList.of(item.toRef()));
                container.setThisOrChildLastUpdated(item.toRef().getUpdated());
                container.setUpcomingContent(upcomingBroadcasts);
                container.setAvailableContent(availableLocations);
                container.setItemSummaries(ImmutableList.of(episode.toSummary()));
                ColumnListMutation<String> mutation = batch.withRow(mainCf, rowId);
                marshaller.marshallInto(episode.getSeriesRef().getId(), mutation, container, false);
            }
            batch.execute();

        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected void doWriteBroadcast(ItemRef itemRef, Optional<ContainerRef> containerRef, Optional<SeriesRef> seriesRef, Broadcast broadcast) {
        Item item = new Item();
        item.setId(itemRef.getId());
        item.setThisOrChildLastUpdated(itemRef.getUpdated());
        item.setPublisher(itemRef.getSource());
        item.addBroadcast(broadcast);
        MutationBatch batch = keyspace.prepareMutationBatch();
        batch.setConsistencyLevel(writeConsistency);
        ColumnListMutation<String> itemMutation = batch.withRow(mainCf, itemRef.getId().longValue());
        marshaller.marshallInto(item.getId(), itemMutation, item, false);

        try {
            if (!broadcast.isActivelyPublished() || !item.getUpcomingBroadcastRefs().iterator().hasNext()) {
                batch.execute();
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
                ColumnListMutation<String> containerMutation = batch.withRow(mainCf, containerRef.get().getId().longValue());
                marshaller.marshallInto(containerId, containerMutation, container, false);
            }

            if (seriesRef.isPresent()) {
                Id containerId = seriesRef.get().getId();
                Container container = new Series();
                container.setItemRefs(ImmutableList.of(itemRef));
                container.setThisOrChildLastUpdated(itemRef.getUpdated());
                container.setUpcomingContent(upcomingBroadcasts);
                ColumnListMutation<String> seriesMutation = batch.withRow(mainCf, seriesRef.get().getId().longValue());
                marshaller.marshallInto(containerId, seriesMutation, container, false);
            }


            batch.execute();
        } catch (ConnectionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected void writeContainerSummary(ContainerSummary summary, Iterable<ItemRef> items) {
        MutationBatch batch = keyspace.prepareMutationBatch();
        batch.setConsistencyLevel(writeConsistency);
        for (ItemRef itemRef : items) {
            Item item = itemFromRef(itemRef);
            item.setContainerSummary(summary);
            ColumnListMutation<String> itemMutation = batch.withRow(mainCf, itemRef.getId().longValue());
            marshaller.marshallInto(itemRef.getId(), itemMutation, item, false);
        }

        try {
            batch.execute();
        } catch (ConnectionException e) {
            Throwables.propagate(e);
        }

    }

    @Override
    protected void removeAllReferencesToItem(ContainerRef containerRef, ItemRef itemRef) {
        MutationBatch batch = keyspace.prepareMutationBatch();
        batch.setConsistencyLevel(writeConsistency);
        removeAllReferencesToItem(containerRef, itemRef, batch);
        try {
            batch.execute();
        } catch (ConnectionException e) {
            Throwables.propagate(e);
        }
    }

    private void removeItemRefsFromContainers(Item item) throws ConnectionException {
        MutationBatch batch = keyspace.prepareMutationBatch();
        batch.setConsistencyLevel(writeConsistency);
        if (item.getContainerRef() != null) {
            removeAllReferencesToItem(item.getContainerRef(), item.toRef(), batch);
        }
        if(item instanceof Episode && ((Episode) item).getSeriesRef() != null){
            Episode episode = (Episode) item;
            removeAllReferencesToItem(episode.getSeriesRef(), episode.toRef(), batch);
        }
        batch.execute();
    }

    private void removeAllReferencesToItem(ContainerRef containerRef, ItemRef itemRef, MutationBatch batch) {
        removeContentRef(containerRef, itemRef, batch);
        removeItemSummaries(containerRef, itemRef, batch);
        removeAvailableContent(containerRef, itemRef, batch);
        removeUpcomingContent(containerRef, itemRef, batch);
    }

    private Item itemFromRef(ItemRef itemRef){
        Item item;
        if(itemRef instanceof EpisodeRef) {
            item = new Episode();
        } else{
            item = new Item();
        }
        item.setId(itemRef.getId());
        item.setThisOrChildLastUpdated(itemRef.getUpdated());
        item.setPublisher(itemRef.getSource());
        return item;
    }

    private static class KeyValue<K, V> {
        private final K key;
        private final V value;

        private KeyValue(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }
}
