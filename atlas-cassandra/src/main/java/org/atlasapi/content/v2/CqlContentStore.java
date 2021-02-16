package org.atlasapi.content.v2;

import com.codahale.metrics.MetricRegistry;
import com.codepoetics.protonpack.maps.MapStream;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.Timestamp;
import com.metabroadcast.promise.Promise;
import org.atlasapi.content.Brand;
import org.atlasapi.content.BrandRef;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.BroadcastRef;
import org.atlasapi.content.Container;
import org.atlasapi.content.ContainerRef;
import org.atlasapi.content.ContainerSummary;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.ItemSummary;
import org.atlasapi.content.LocationSummary;
import org.atlasapi.content.Series;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.content.v2.model.udt.Description;
import org.atlasapi.content.v2.model.udt.ItemRefAndBroadcastRefs;
import org.atlasapi.content.v2.model.udt.ItemRefAndItemSummary;
import org.atlasapi.content.v2.model.udt.ItemRefAndLocationSummaries;
import org.atlasapi.content.v2.model.udt.PartialItemRef;
import org.atlasapi.content.v2.model.udt.Ref;
import org.atlasapi.content.v2.serialization.BroadcastRefSerialization;
import org.atlasapi.content.v2.serialization.BroadcastSerialization;
import org.atlasapi.content.v2.serialization.ContainerSummarySerialization;
import org.atlasapi.content.v2.serialization.ContentSerialization;
import org.atlasapi.content.v2.serialization.ContentSerializationImpl;
import org.atlasapi.content.v2.serialization.ItemRefSerialization;
import org.atlasapi.content.v2.serialization.ItemSummarySerialization;
import org.atlasapi.content.v2.serialization.LocationSummarySerialization;
import org.atlasapi.content.v2.serialization.RefSerialization;
import org.atlasapi.content.v2.serialization.SeriesRefSerialization;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.MissingResourceException;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class CqlContentStore implements ContentStore {

    private static final Logger log = LoggerFactory.getLogger(CqlContentStore.class);

    private static final String METER_CALLED = "meter.called";
    private static final String METER_FAILURE = "meter.failure";

    private final Session session;
    private final Mapper<org.atlasapi.content.v2.model.Content> mapper;
    private final ContentAccessor accessor;
    private final IdGenerator idGenerator;
    private final MessageSender<ResourceUpdatedMessage> sender;
    private final Clock clock;

    private final ContentSerialization translator = new ContentSerializationImpl();
    private final BroadcastSerialization broadcastTranslator = new BroadcastSerialization();
    private final RefSerialization refTranslator = new RefSerialization();
    private final SeriesRefSerialization seriesRefTranslator = new SeriesRefSerialization();
    private final ItemRefSerialization itemRefTranslator = new ItemRefSerialization();
    private final BroadcastRefSerialization broadcastRefTranslator =
            new BroadcastRefSerialization();
    private final ItemSummarySerialization itemSummaryTranslator = new ItemSummarySerialization();
    private final LocationSummarySerialization locationSummaryTranslator =
            new LocationSummarySerialization();
    private final ContainerSummarySerialization containerSummaryTranslator =
            new ContainerSummarySerialization();
    private final EquivalenceGraphStore graphStore;

    private final String writeContent;
    private final String writeBroadcast;

    private final MetricRegistry metricRegistry;

    protected CqlContentStore(Builder builder) {
        this.idGenerator = checkNotNull(builder.idGenerator);
        this.session = checkNotNull(builder.session);
        this.clock = checkNotNull(builder.clock);

        MappingManager mappingManager = new MappingManager(session);

        // TODO: bug in driver 3.1.0 prompting this hackaround. Remove when it's fixed. MBST-16715
        mappingManager.udtCodec(Description.class);
        mappingManager.udtCodec(org.atlasapi.content.v2.model.udt.BroadcastRef.class);
        mappingManager.udtCodec(org.atlasapi.content.v2.model.udt.LocationSummary.class);
        mappingManager.udtCodec(org.atlasapi.content.v2.model.udt.LocationSummary.class);
        mappingManager.udtCodec(org.atlasapi.content.v2.model.udt.ItemSummary.class);

        this.mapper = mappingManager.mapper(org.atlasapi.content.v2.model.Content.class);
        this.accessor = mappingManager.createAccessor(ContentAccessor.class);

        mapper.setDefaultGetOptions(Mapper.Option.consistencyLevel(builder.readConsistency));
        mapper.setDefaultSaveOptions(Mapper.Option.consistencyLevel(builder.writeConsistency));
        mapper.setDefaultDeleteOptions(Mapper.Option.consistencyLevel(builder.writeConsistency));

        this.sender = builder.sender;
        this.graphStore = checkNotNull(builder.graphStore);

        writeContent = builder.metricPrefix + "writeContent.";
        writeBroadcast = builder.metricPrefix + "writeBroadcast.";

        this.metricRegistry = builder.metricRegistry;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public WriteResult<Content, Content> writeContent(Content content) throws WriteException {
        metricRegistry.meter(writeContent + METER_CALLED).mark();
        try {

            org.atlasapi.content.v2.model.Content previousSerialized
                    = resolvePreviousSerialized(content);
            Content previous = deserializeIfFull(previousSerialized);

            BatchStatement batch = new BatchStatement();

            Container container = resolveContainer(content);
            ensureContentSummary(content, container);
            ensureId(content);
            filterUnpublishedBroadcasts(content);

            ImmutableList.Builder<ResourceUpdatedMessage> messages = ImmutableList.builder();

            DateTime now = clock.now();

            batch.addAll(updateWriteDates(content, now));
            batch.addAll(deleteContainerSummary(previous, content, messages));
            try {
                batch.addAll(updateContainerDenormalizedInfo(content, previous, messages));
            } catch (Exception e) {
                String contentInfo = String.format(
                        "Content - {title: %s, id: %d, type: %s}",
                        content.getTitle(),
                        content.getId().longValue(),
                        content.getClass().getSimpleName()
                );
                String previousInfo = previous != null ? String.format(
                        "Previous - {title: %s, id: %d, type: %s}",
                        previous.getTitle(),
                        previous.getId().longValue(),
                        previous.getClass().getSimpleName()
                )
                                                       : "null";
                log.error(
                        "Error with updateContainerDenormalizedInfo: {} {}",
                        contentInfo,
                        previousInfo,
                        e
                );
                throw new RuntimeException(e); //rethrow to be caught by metrics
            }
            batch.addAll(updateChildrenSummaries(content, previous, messages));

            setExistingItemRefs(content, previous);

            org.atlasapi.content.v2.model.Content serialized = translator.serialize(content);

            boolean unchanged = serialized.equals(previousSerialized);

            if (unchanged) {
                return WriteResult.<Content, Content>result(content, false)
                        .withPrevious(previous)
                        .build();
            }

            batch.add(mapper.saveQuery(serialized));

            session.execute(batch);

            messages.add(new ResourceUpdatedMessage(
                    UUID.randomUUID().toString(),
                    Timestamp.of(DateTime.now()),
                    content.toRef()
            ));

            sendMessages(messages.build());


            return new WriteResult<>(content, true, DateTime.now(), previous);
        } catch (WriteException | RuntimeException e) {
            metricRegistry.meter(writeContent + METER_FAILURE).mark();
            throw e;
        }
    }

    private void filterUnpublishedBroadcasts(Content content) {
        if (content instanceof Item) {
            Item item = (Item) content;
            item.setBroadcasts(
                    item.getBroadcasts()
                            .stream()
                            .filter(Broadcast::isActivelyPublished)
                            .collect(MoreCollectors.toImmutableSet())
            );
        }
    }

    @Override
    public void writeBroadcast(
            ItemRef item,
            Optional<ContainerRef> containerRef,
            Optional<SeriesRef> seriesRef,
            Broadcast broadcast
    ) {
        metricRegistry.meter(writeBroadcast + METER_CALLED).mark();
        try {
            BatchStatement batch = new BatchStatement();

            Instant now = clock.now().toInstant();

            org.atlasapi.content.v2.model.udt.Broadcast serialized =
                    broadcastTranslator.serialize(broadcast);

            ImmutableMap<String, org.atlasapi.content.v2.model.udt.Broadcast> broadcasts =
                    ImmutableMap.of(broadcast.getSourceId(), serialized);

            batch.add(accessor.addBroadcastToContent(
                    item.getId().longValue(),
                    broadcasts
            ));
            batch.add(accessor.setLastUpdated(item.getId().longValue(), now));

            // we only denormalize upcoming broadcasts on containers. If this is stale, just add to
            // item and return early
            if (broadcast.isActivelyPublished() && broadcast.isUpcoming()) {
                Ref ref = refTranslator.serialize(item);
                PartialItemRef itemRef = itemRefTranslator.serialize(item);

                Map<Ref, ItemRefAndBroadcastRefs> upcomingBroadcasts =
                        ImmutableMap.of(
                                ref,
                                new ItemRefAndBroadcastRefs(
                                        itemRef,
                                        ImmutableList.of(broadcastRefTranslator.serialize(broadcast.toRef()))
                                )
                        );

                if (containerRef.isPresent()) {
                    long id = containerRef.get().getId().longValue();

                    batch.add(accessor.addItemRefsToContainer(
                            id,
                            ImmutableMap.of(),
                            upcomingBroadcasts,
                            ImmutableMap.of()
                    ));
                    batch.add(accessor.setLastUpdated(id, now));
                }

                if (seriesRef.isPresent()) {
                    long id = seriesRef.get().getId().longValue();
                    batch.add(accessor.addItemRefsToContainer(
                            id,
                            ImmutableMap.of(ref, itemRef),
                            upcomingBroadcasts,
                            ImmutableMap.of()
                    ));
                    batch.add(accessor.setLastUpdated(id, now));
                }
            }

            session.execute(batch);

            sendMessages(ImmutableList.of(new ResourceUpdatedMessage(
                    UUID.randomUUID().toString(),
                    Timestamp.of(clock.now()),
                    item
            )));
        } catch (RuntimeException e) {
            metricRegistry.meter(writeBroadcast + METER_FAILURE).mark();
            Throwables.propagate(e);
        }
    }

    private org.atlasapi.content.v2.model.Content resolvePreviousSerialized(
            @Nullable Content content
    ) throws WriteException {

        org.atlasapi.content.v2.model.Content previous;
        if (content != null && content.getId() != null) {
            try {
                previous = accessor.getContent(content.getId().longValue()).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new WriteException(
                        String.format("Failed to resolve content %s", content.getId()),
                        e
                );
            }
        } else {
            previous = null;
        }
        return previous;
    }

    @Override
    public ListenableFuture<Resolved<Content>> resolveIds(Iterable<Id> ids) {
        List<ListenableFuture<Content>> futures = StreamSupport.stream(
                ids.spliterator(),
                false
        )
                .map(Id::longValue)
                .map(accessor::getContent)
                .map(contentFuture -> Promise.wrap(contentFuture).then(this::deserializeIfFull))
                .collect(Collectors.toList());

        ListenableFuture<List<Content>> contentList = Futures.transform(
                Futures.allAsList(futures),
                (List<Content> input) -> input.stream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList())
        );

        return Futures.transform(
                contentList,
                (Function<List<Content>, Resolved<Content>>) Resolved::valueOf
        );
    }

    @Nullable
    private Content deserializeIfFull(@Nullable org.atlasapi.content.v2.model.Content content) {
        // we denormalise things like itemRefs on containers. When a content gets written, we write
        // its itemRef onto the container without checking that the container exists, thus
        // creating an incomplete container object. These can be recognised most notably by
        // lack of 'type'
        if (content != null && content.getType() != null) {
            return translator.deserialize(content);
        } else {
            return null;
        }
    }

    protected void sendMessages(ImmutableList<ResourceUpdatedMessage> messages) {
        if (sender == null) {
            return;
        }

        Map<Id, Id> resourceGraphIds = getResourceGraphIds(messages);

        for (ResourceUpdatedMessage message : messages) {
            try {
                // Downstream workers are processing the entire equivalence graph for every
                // updated content. Therefore we want to use the graph ID as the partition key
                // to ensure each graph is only processed by a single worker thread.
                // We default to using the updated resource id as the partition key if we can't
                // resolve the resource's graph.
                Id resourceId = message.getUpdatedResource().getId();

                Id partitionId =
                        resourceGraphIds.containsKey(resourceId)
                        ? resourceGraphIds.get(resourceId)
                        : resourceId;

                sender.sendMessage(message, Longs.toByteArray(partitionId.longValue()));
            } catch (Exception e) {
                log.error("Failed to send message " + message.getUpdatedResource().toString(), e);
            }
        }
    }

    private Map<Id, Id> getResourceGraphIds(Iterable<ResourceUpdatedMessage> messages) {
        ImmutableSet<Id> messageResourceIds = StreamSupport.stream(messages.spliterator(), false)
                .map(message -> message.getUpdatedResource().getId())
                .distinct()
                .collect(MoreCollectors.toImmutableSet());

        return getResourceGraphIds(
                messageResourceIds,
                getGraphMap(messageResourceIds)
        );
    }

    private ImmutableMap<Id, Id> getResourceGraphIds(
            Iterable<Id> updatedResourceIds,
            OptionalMap<Id, EquivalenceGraph> graphMap
    ) {
        return StreamSupport.stream(updatedResourceIds.spliterator(), false)
                .distinct()
                .filter(resourceId -> graphMap.get(resourceId).isPresent())
                .collect(MoreCollectors.toImmutableMap(
                        java.util.function.Function.identity(),
                        resourceId -> graphMap.get(resourceId)
                                .get()
                                .getId()
                ));
    }

    private OptionalMap<Id, EquivalenceGraph> getGraphMap(Iterable<Id> messageResourceIds) {
        try {
            return graphStore.resolveIds(messageResourceIds).get();
        } catch (InterruptedException | ExecutionException e) {
            return ImmutableOptionalMap.of();
        }
    }

    private void setExistingItemRefs(Content content, Content previous) {
        if (content instanceof Container && previous instanceof Container) {
            Container previousContainer = (Container) previous;
            Container currentContainer = (Container) content;

            currentContainer.setItemRefs(previousContainer.getItemRefs());
            currentContainer.setItemSummaries(previousContainer.getItemSummaries());

            if (content instanceof Brand && previousContainer instanceof Brand) {
                Brand previousBrand = (Brand) previousContainer;
                Brand currentBrand = (Brand) currentContainer;
                currentBrand.setSeriesRefs(previousBrand.getSeriesRefs());
            }
        }
    }

    /** Updates the denormalized container summaries on all children
     *
     * We use the itemRefs from {@code previous} because we can't rely on the ones in the content
     * that got passed in. The itemRefs are written on the container when the children are updated,
     * which means that they should be read only. If we get passed in a bunch of itemRefs, we don't
     * know if those IDs exist in the DB, therefore we ignore them and use the ones we resolved from
     * DB, aka {@code previous}.
     *
     * @param content content being written
     * @param previous previous content, resolved from DB
     * @param messages input arg of resource update messages to send
     * @return a bunch of update statements to execute
     */
    private Iterable<? extends Statement> updateChildrenSummaries(
            Content content,
            @Nullable Content previous,
            ImmutableList.Builder<ResourceUpdatedMessage> messages
    ) {
        List<Statement> statements = Lists.newArrayList();

        if (content instanceof Container) {
            Container container = (Container) content;

            if (previous != null && previous instanceof Container) {
                Container previousContainer = (Container) previous;
                ContainerSummary currentSummary = container.toSummary();
                ContainerSummary previousSummary = previousContainer.toSummary();

                if (currentSummary.equals(previousSummary)) {
                    return ImmutableList.of();
                }

                Iterable<ItemRef> itemRefs = previousContainer.getItemRefs();

                for (ItemRef childRef : itemRefs) {
                    statements.add(accessor.updateContainerSummary(
                            childRef.getId().longValue(),
                            containerSummaryTranslator.serialize(container.toSummary())
                    ));

                    messages.add(new ResourceUpdatedMessage(
                            UUID.randomUUID().toString(),
                            Timestamp.of(DateTime.now()),
                            childRef
                    ));
                }
            }

        }

        return statements;
    }

    protected Content resolvePrevious(Content content) throws WriteException {
        Content previous;
        if (content != null && content.getId() != null) {
            try {
                previous = Iterables.getOnlyElement(
                        resolveIds(ImmutableList.of(content.getId()))
                                .get().getResources(),
                        null
                );
            } catch (InterruptedException | ExecutionException e) {
                throw new WriteException(
                        String.format("Failed to resolve content %s", content.getId()),
                        e
                );
            }
        } else {
            previous = null;
        }
        return previous;
    }

    @Nullable
    private Container resolveContainer(Content content) throws WriteException {
        ContainerRef containerRef = null;

        if (content instanceof Item) {
            Item item = (Item) content;
            containerRef = item.getContainerRef();
        }

        if (content instanceof Series) {
            Series series = (Series) content;
            containerRef = series.getBrandRef();
        }

        if (containerRef != null) {
            try {
                Optional<Content> container = resolveIds(ImmutableList.of(containerRef.getId()))
                        .get()
                        .getResources()
                        .first();
                if (container.isPresent()) {
                    return (Container) container.get();
                } else {
                    throw new WriteException(
                            String.format("Failed to resolve container %s for %s",
                                    containerRef.getId(), content.getId()
                            ),
                            new MissingResourceException(containerRef.getId())
                    );
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new WriteException(
                        String.format(
                                "Failed to retrieve container %s for %s seconds",
                                containerRef.getId(),
                                content.getId()
                        ),
                        e
                );
            }
        }

        return null;
    }

    private void ensureContentSummary(Content content, Container container) throws WriteException {
        if (content instanceof Item) {
            Item item = (Item) content;
            if (container != null) {
                item.setContainerSummary(container.toSummary());
            }
        }
    }

    private List<Statement> deleteContainerSummary(
            @Nullable Content previous,
            Content content,
            ImmutableList.Builder<ResourceUpdatedMessage> messages
    ) {
        if (containerWasRemoved(previous, content)) {
            return ImmutableList.of(
                    accessor.deleteContainerSummary(
                            content.getId().longValue()
                    )
            );
        }

        return ImmutableList.of();
    }

    private boolean containerWasRemoved(@Nullable Content previous, Content content) {
        return previous != null
                && previous instanceof Item
                && content instanceof Item
                && ((Item) previous).getContainerRef() != null
                && ((Item) content).getContainerRef() == null;
    }

    private List<Statement> updateWriteDates(Content content, DateTime now) {
        content.setLastUpdated(now);

        if (content.getFirstSeen() == null) {
            content.setFirstSeen(now);
        }

        content.setThisOrChildLastUpdated(now);

        List<Statement> result = Lists.newArrayList();

        if (content instanceof Item) {
            Item item = (Item) content;
            ContainerRef container = item.getContainerRef();
            if (container != null) {
                result.add(accessor.setLastUpdated(container.getId().longValue(), now.toInstant()));
            }
        }

        if (content instanceof Episode) {
            Episode episode = (Episode) content;
            SeriesRef series = episode.getSeriesRef();
            if (series != null) {
                result.add(accessor.setLastUpdated(series.getId().longValue(), now.toInstant()));
            }
        }

        if (content instanceof Series) {
            Series series = (Series) content;
            BrandRef brand = series.getBrandRef();
            if (brand != null) {
                result.add(accessor.setLastUpdated(brand.getId().longValue(), now.toInstant()));
            }
        }

        return result;
    }

    private List<Statement> updateContainerDenormalizedInfo(
            Content content,
            Content previous,
            ImmutableList.Builder<ResourceUpdatedMessage> messages
    ) throws Exception {
        List<Statement> result = Lists.newArrayList();

        if (content instanceof Series) {
            // update brand series ref
            Series series = (Series) content;
            BrandRef brandRef = series.getBrandRef();

            if (brandRef != null) {
                if (!series.isActivelyPublished()) {
                    result.add(accessor.removeSeriesRefFromBrand(
                            brandRef.getId().longValue(),
                            ImmutableSet.of(refTranslator.serialize(series.toRef()))
                    ));
                } else {
                    result.add(accessor.addSeriesRefToBrand(
                            brandRef.getId().longValue(),
                            ImmutableMap.of(
                                    refTranslator.serialize(series.toRef()),
                                    seriesRefTranslator.serialize(series.toRef())
                            )
                    ));
                }

                messages.add(new ResourceUpdatedMessage(
                        UUID.randomUUID().toString(),
                        Timestamp.of(DateTime.now()),
                        brandRef
                ));
            }
        }

        if (content instanceof Item) {
            Item item = (Item) content;
            Item previousItem = null;
            if (previous instanceof Item) {
                previousItem = (Item) previous;
            }

            ContainerRef containerRef = item.getContainerRef();
            ContainerRef previousContainerRef = previousItem != null
                                                ? previousItem.getContainerRef()
                                                : null;

            SeriesRef seriesRef = null;
            SeriesRef previousSeriesRef = null;

            if (content instanceof Episode) {
                Episode episode = (Episode) content;
                seriesRef = episode.getSeriesRef();
            }

            if (previousItem instanceof Episode) {
                Episode previousEpisode = (Episode) previousItem;
                previousSeriesRef = previousEpisode.getSeriesRef();
            }

            if (!Objects.equals(containerRef, previousContainerRef)) {
                result.addAll(removeItemRefsFromContainers(item, previousContainerRef));
            }

            if (!Objects.equals(seriesRef, previousSeriesRef)) {
                result.addAll(removeItemRefsFromContainers(item, previousSeriesRef));
            }

            if (!item.isActivelyPublished()
                    || (item.isGenericDescription() != null
                    && item.isGenericDescription())) {
                result.addAll(removeItemRefsFromContainers(item, containerRef, seriesRef));
                return result;
            }

            result.addAll(addItemRefsToContainers(messages, item, containerRef, seriesRef));
        }

        return result;
    }

    private List<Statement> addItemRefsToContainers(
            ImmutableList.Builder<ResourceUpdatedMessage> messages,
            Item item,
            ContainerRef containerRef,
            SeriesRef seriesRef
    ) {
        List<Statement> result = Lists.newArrayList();

        ItemRef ref = item.toRef();
        ItemSummary summary = item.toSummary();

        PartialItemRef itemRef = itemRefTranslator.serialize(ref);
        Ref itemRefKey = refTranslator.serialize(ref);
        org.atlasapi.content.v2.model.udt.ItemSummary itemSummary = itemSummaryTranslator.serialize(summary);

        ImmutableMap<ItemRef, Iterable<BroadcastRef>> upcoming = ImmutableMap.of(
                item.toRef(), item.getUpcomingBroadcastRefs()
        );

        Map<
                Ref,
                ItemRefAndBroadcastRefs
        > upcomingSerialised = MapStream
                .of(upcoming)
                .mapEntries(
                        refTranslator::serialize,
                        vals -> new ItemRefAndBroadcastRefs(
                                    itemRef,
                                    StreamSupport.stream(vals.spliterator(), false)
                                        .map(broadcastRefTranslator::serialize)
                                        .collect(Collectors.toList()))
                )
                .collect();

        ImmutableMap<ItemRef, Iterable<LocationSummary>> availableLocations = ImmutableMap.of(
                item.toRef(), item.getAvailableLocations()
        );

        Map<
                Ref,
                ItemRefAndLocationSummaries
        > availableLocationsSerialised = MapStream
                .of(availableLocations)
                .mapEntries(
                        refTranslator::serialize,
                        vals -> new ItemRefAndLocationSummaries(
                                itemRef,
                                StreamSupport.stream(vals.spliterator(), false)
                                        .map(locationSummaryTranslator::serialize)
                                        .collect(Collectors.toList()))
                )
                .collect();


        if (containerRef != null) {
            result.add(accessor.addItemRefsToContainer(
                    containerRef.getId().longValue(),
                    ImmutableMap.of(itemRefKey, itemRef),
                    upcomingSerialised,
                    availableLocationsSerialised
            ));

            if (seriesRef == null) {
                result.add(accessor.addItemSummariesToContainer(
                        containerRef.getId().longValue(),
                        ImmutableMap.of(itemRefKey, new ItemRefAndItemSummary(itemRef, itemSummary))
                ));
            }

            messages.add(new ResourceUpdatedMessage(
                    UUID.randomUUID().toString(),
                    Timestamp.of(DateTime.now()),
                    containerRef
            ));
        }

        if (seriesRef != null) {
            result.add(accessor.addItemRefsToContainer(
                    seriesRef.getId().longValue(),
                    ImmutableMap.of(itemRefKey, itemRef),
                    upcomingSerialised,
                    availableLocationsSerialised
            ));

            result.add(accessor.addItemSummariesToContainer(
                    seriesRef.getId().longValue(),
                    ImmutableMap.of(itemRefKey, new ItemRefAndItemSummary(itemRef, itemSummary))
            ));

            messages.add(new ResourceUpdatedMessage(
                    UUID.randomUUID().toString(),
                    Timestamp.of(DateTime.now()),
                    seriesRef
            ));
        }

        return result;
    }

    private Collection<? extends Statement> removeItemRefsFromContainers(
            Item item,
            ContainerRef... containerRefs
    ) {
        ItemRef ref = item.toRef();

        ImmutableMap<ItemRef, Iterable<BroadcastRef>> upcoming = ImmutableMap.of(
                item.toRef(), item.getUpcomingBroadcastRefs()
        );

        Set<Ref> upcomingSerialised = upcoming.keySet()
                .stream()
                .map(refTranslator::serialize)
                .collect(MoreCollectors.toImmutableSet());

        ImmutableMap<ItemRef, Iterable<LocationSummary>> availableLocations = ImmutableMap.of(
                item.toRef(), item.getAvailableLocations()
        );

        Set<Ref> availableLocationsSerialised = availableLocations
                .keySet()
                .stream()
                .map(refTranslator::serialize)
                .collect(MoreCollectors.toImmutableSet());

        Ref itemRefKey = refTranslator.serialize(ref);

        return Arrays.stream(containerRefs)
                .filter(Objects::nonNull)
                .flatMap(containerRef -> Lists.newArrayList(
                        accessor.removeItemRefsFromContainer(
                                containerRef.getId().longValue(),
                                ImmutableSet.of(itemRefKey),
                                upcomingSerialised,
                                availableLocationsSerialised
                        ),
                        accessor.removeItemSummariesFromContainer(
                                containerRef.getId().longValue(),
                                ImmutableSet.of(itemRefKey)
                        )
                ).stream()).collect(Collectors.toList());
    }

    private void ensureId(Content content) {
        if (content.getId() == null) {
            content.setId(Id.valueOf(idGenerator.generateRaw()));
        }
    }

    public static final class Builder {

        private Session session;
        private IdGenerator idGenerator;
        private MessageSender<ResourceUpdatedMessage> sender;
        private Clock clock;
        private EquivalenceGraphStore graphStore;
        private MetricRegistry metricRegistry;
        private String metricPrefix;
        private ConsistencyLevel readConsistency = ConsistencyLevel.QUORUM;
        private ConsistencyLevel writeConsistency = ConsistencyLevel.QUORUM;

        private Builder() {}

        public Builder withSession(Session val) {
            session = val;
            return this;
        }

        public Builder withIdGenerator(IdGenerator val) {
            idGenerator = val;
            return this;
        }

        public Builder withSender(MessageSender<ResourceUpdatedMessage> val) {
            sender = val;
            return this;
        }

        public Builder withClock(Clock val) {
            clock = val;
            return this;
        }

        public Builder withGraphStore(EquivalenceGraphStore val) {
            graphStore = val;
            return this;
        }

        public Builder withMetricRegistry(MetricRegistry val) {
            metricRegistry = val;
            return this;
        }

        public Builder withMetricPrefix(String val) {
            metricPrefix = val;
            return this;
        }

        public Builder withReadConsistency(ConsistencyLevel val) {
            readConsistency = val;
            return this;
        }

        public Builder withWriteConsistency(ConsistencyLevel val) {
            writeConsistency = val;
            return this;
        }

        public CqlContentStore build() {
            return new CqlContentStore(this);
        }
    }
}
