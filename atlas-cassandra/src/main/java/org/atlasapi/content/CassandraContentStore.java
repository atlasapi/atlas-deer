package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.content.ContentColumn.DESCRIPTION;
import static org.atlasapi.content.ContentColumn.IDENTIFICATION;
import static org.atlasapi.content.ContentColumn.SOURCE;
import static org.atlasapi.content.ContentColumn.TYPE;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.collect.Sets;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.AliasIndex;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.util.CassandraUtil;
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
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public final class CassandraContentStore extends AbstractContentStore {
    
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

        public CassandraContentStore build() {
            return new CassandraContentStore(context, name, readCl, writeCl,
                hasher, idGenerator, sender, clock);
        }

    }

    private static final Set<ContentColumn> REQUIRED_CONTENT_COLUMNS = ImmutableSet.of(TYPE, SOURCE, IDENTIFICATION);

    private final Keyspace keyspace;
    private final ConsistencyLevel readConsistency;
    private final ConsistencyLevel writeConsistency;
    private final ColumnFamily<Long, String> mainCf;
    private final AliasIndex<Content> aliasIndex;

    private final ContentMarshaller marshaller = new ProtobufContentMarshaller();
    private final Function<Row<Long, String>, Content> rowToContent =
            input -> {
                if (!input.getColumns().isEmpty()) {
                    verifyRequiredColumns(input.getKey(), input.getColumns());
                    return marshaller.unmarshallCols(input.getColumns());
                }
                return null;
            };

    private void verifyRequiredColumns(Long id, ColumnList<String> columns) {
        for (ContentColumn requiredColumn : REQUIRED_CONTENT_COLUMNS) {
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

    private final Function<Rows<Long, String>, Resolved<Content>> toResolvedContent =
            rows -> Resolved.valueOf(FluentIterable.from(rows).transform(rowToContent).filter(Predicates.notNull()));

    public CassandraContentStore(AstyanaxContext<Keyspace> context,
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

    private ListenableFuture<Rows<Long, String>> resolveLongs(Iterable<Long> longIds) throws ConnectionException {
        return Futures.transform(keyspace
            .prepareQuery(mainCf)
            .setConsistencyLevel(readConsistency)
            .getKeySlice(longIds)
            .executeAsync(), CassandraUtil.<Rows<Long, String>>toResult());
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
            Rows<Long,String> resolved = resolveLongs(ids).get(10, TimeUnit.SECONDS);
            Iterable<Content> contents = Iterables.transform(resolved, rowToContent);
            ImmutableMap.Builder<Alias, Optional<Content>> aliasMap = ImmutableMap.builder();
            for (Content content : contents) {
                for (Alias alias : content.getAliases()) {
                    if (uniqueAliases.contains(alias)) {
                        aliasMap.put(alias, Optional.of(content));
                    }
                }
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
            marshaller.marshallInto(batch.withRow(mainCf, id), content);
            batch.mergeShallow(aliasIndex.mutateAliases(content, previous));
            batch.execute();
            log.trace("Written content id " + id);
        } catch (Exception e) {
            throw new CassandraPersistenceException(content.toString(), e);
        }
    }
    
    @Override
    protected @Nullable
    Content resolvePrevious(@Nullable Id id, Publisher source, Set<Alias> aliases) {
        Content previous = null;
        try {
            if (id != null) {
                previous = resolve(id.longValue(), null);
            } else {
                    Set<Long> ids = aliasIndex.readAliases(source, aliases);
                    Long aliasId = Iterables.getFirst(ids, null);
                    if (aliasId != null) {
                        previous = resolve(aliasId, null);
                    }
            }

        } catch (ConnectionException e) {
            throw Throwables.propagate(e);
        } catch (CorruptContentException e) {
            log.error("Previously written content is corrupt", e);
        }
        return previous;
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
    protected void writeSecondaryContainerRef(BrandRef primary, SeriesRef seriesRef) {
        try {
            Long rowId = primary.getId().longValue();
            Brand container = new Brand();
            container.setSeriesRefs(ImmutableList.of(seriesRef));
            container.setThisOrChildLastUpdated(seriesRef.getUpdated());
            
            MutationBatch batch = keyspace.prepareMutationBatch();
            batch.setConsistencyLevel(writeConsistency);
            ColumnListMutation<String> mutation = batch.withRow(mainCf, rowId);
            marshaller.marshallInto(mutation, container);
            batch.execute();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected void writeItemRef(
            ContainerRef containerRef,
            ItemRef childRef,
            Iterable<BroadcastRef> upcomingContent,
            Iterable<LocationSummary> availableContent
    ) {
        try {

            Long rowId = containerRef.getId().longValue();
            Container container = new Brand();
            container.setItemRefs(ImmutableList.of(childRef));
            container.setThisOrChildLastUpdated(childRef.getUpdated());
            container.setUpcomingContent(ImmutableMap.of(childRef, upcomingContent));
            container.setAvailableContent(ImmutableMap.of(childRef, availableContent));

            MutationBatch batch = keyspace.prepareMutationBatch();
            batch.setConsistencyLevel(writeConsistency);
            ColumnListMutation<String> mutation = batch.withRow(mainCf, rowId);
            marshaller.marshallInto(mutation, container);
            batch.execute();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

}
