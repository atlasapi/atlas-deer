package org.atlasapi.content.v2;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.content.BrandRef;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Container;
import org.atlasapi.content.ContainerRef;
import org.atlasapi.content.ContainerSummary;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.ItemSummary;
import org.atlasapi.content.Series;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.content.v2.serialization.ContainerSummarySerialization;
import org.atlasapi.content.v2.serialization.ContentSerialization;
import org.atlasapi.content.v2.serialization.ContentSerializationImpl;
import org.atlasapi.content.v2.serialization.ItemRefSerialization;
import org.atlasapi.content.v2.serialization.ItemSummarySerialization;
import org.atlasapi.content.v2.serialization.SeriesRefSerialization;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.MissingResourceException;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.messaging.ResourceUpdatedMessage;

import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessagingException;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.Timestamp;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class CqlContentStore implements ContentStore {

    private static final Logger log = LoggerFactory.getLogger(CqlContentStore.class);

    private static final long READ_TIMEOUT_SECONDS = 5L;

    private final Session session;
    private final Mapper<org.atlasapi.content.v2.model.Content> mapper;
    private final ContentAccessor accessor;
    private final IdGenerator idGenerator;
    private final MessageSender<ResourceUpdatedMessage> sender;
    private final Clock clock;

    private final ContentSerialization translator = new ContentSerializationImpl();
    private final SeriesRefSerialization seriesRefTranslator = new SeriesRefSerialization();
    private final ItemRefSerialization itemRefTranslator = new ItemRefSerialization();
    private final ItemSummarySerialization itemSummaryTranslator = new ItemSummarySerialization();
    private final ContainerSummarySerialization containerSummaryTranslator =
            new ContainerSummarySerialization();

    public CqlContentStore(
            Session session,
            MessageSender<ResourceUpdatedMessage> sender,
            IdGenerator idGenerator,
            Clock clock
    ) {
        this.idGenerator = checkNotNull(idGenerator);
        this.session = checkNotNull(session);
        this.clock = checkNotNull(clock);

        MappingManager mappingManager = new MappingManager(session);
        this.mapper = mappingManager.mapper(org.atlasapi.content.v2.model.Content.class);
        this.accessor = mappingManager.createAccessor(ContentAccessor.class);

        this.sender = checkNotNull(sender);
    }

    @Override
    public WriteResult<Content, Content> writeContent(Content content) throws WriteException {
        checkArgument(
                !(content instanceof Episode) || ((Episode) content).getContainerRef() != null,
                "Can't write episode without brand"
        );

        Content previous = resolvePrevious(content);

        BatchStatement batch = new BatchStatement();

        Container container = resolveContainer(content);
        ensureContentSummary(content, container);
        ensureId(content);

        ImmutableList.Builder<ResourceUpdatedMessage> messages = ImmutableList.builder();

        DateTime now = clock.now();

        batch.addAll(updateWriteDates(content, now));
        batch.addAll(updateContainerSummary(content, messages));
        batch.addAll(updateParentRefs(content, messages));
        batch.addAll(updateChildrenSummaries(content, previous, messages));

        org.atlasapi.content.v2.model.Content serialized = translator.serialize(content);

        batch.add(mapper.saveQuery(serialized));

        session.execute(batch);

        messages.add(new ResourceUpdatedMessage(
                UUID.randomUUID().toString(),
                Timestamp.of(DateTime.now()),
                content.toRef()
        ));

        for (ResourceUpdatedMessage message : messages.build()) {
            try {
                sender.sendMessage(
                        message,
                        Longs.toByteArray(message.getUpdatedResource().getId().longValue())
                );
            } catch (MessagingException e) {
                log.error("Failed to send message " + message.getUpdatedResource().toString(), e);
            }
        }

        return new WriteResult<>(content, true, DateTime.now(), previous);
    }

    private Iterable<? extends Statement> updateChildrenSummaries(
            Content content,
            Content previous, ImmutableList.Builder<ResourceUpdatedMessage> messages
    ) {
        List<Statement> statements = Lists.newArrayList();

        if (content instanceof Container) {
            Container container = (Container) content;
            Container previousContainer = (Container) previous;

            Iterable<ItemRef> itemRefs = container.getItemRefs();

            if (previousContainer != null) {
                ContainerSummary currentSummary = container.toSummary();
                ContainerSummary previousSummary = previousContainer.toSummary();

                if (currentSummary.equals(previousSummary)) {
                    return ImmutableList.of();
                }

                itemRefs = previousContainer.getItemRefs();
            }

            for (ItemRef childRef : itemRefs) {
                statements.add(accessor.updateContainerSummaryInChild(
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

        return statements;
    }

    private Content resolvePrevious(Content content) throws WriteException {
        Content previous;
        if (content != null && content.getId() != null) {
            try {
                previous = Iterables.getOnlyElement(
                        resolveIds(ImmutableList.of(content.getId()))
                                .get(
                                        READ_TIMEOUT_SECONDS,
                                        TimeUnit.SECONDS
                                ).getResources()
                );
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
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
                        .get(
                                READ_TIMEOUT_SECONDS,
                                TimeUnit.SECONDS
                        )
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
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new WriteException(String.format(
                        "Failed to retrieve container for %s in %d seconds",
                        content.getId(),
                        READ_TIMEOUT_SECONDS
                ));
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

    private List<Statement> updateContainerSummary(
            Content content,
            ImmutableList.Builder<ResourceUpdatedMessage> messages
    ) {
        if (content instanceof Container) {
            Container container = (Container) content;
            ContainerSummary summary = container.toSummary();

            org.atlasapi.content.v2.model.udt.ContainerSummary internal =
                    containerSummaryTranslator.serialize(summary);

            List<Statement> statements = Lists.newArrayList();

            for (ItemRef ir : container.getItemRefs()) {
                messages.add(new ResourceUpdatedMessage(
                        UUID.randomUUID().toString(),
                        Timestamp.of(DateTime.now()),
                        ir
                ));

                statements.add(accessor.updateContainerSummaryInChild(
                        ir.getId().longValue(),
                        internal
                ));
            }

            return statements;
        }

        return ImmutableList.of();
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

    private List<Statement> updateParentRefs(
            Content content,
            ImmutableList.Builder<ResourceUpdatedMessage> messages
    ) {
        List<Statement> result = Lists.newArrayList();

        if (content instanceof Series) {
            // update brand series ref
            Series series = (Series) content;
            BrandRef brandRef = series.getBrandRef();
            if (brandRef != null) {
                result.add(accessor.addSeriesRefToBrand(
                        brandRef.getId().longValue(),
                        ImmutableSet.of(seriesRefTranslator.serialize(series.toRef()))
                ));

                messages.add(new ResourceUpdatedMessage(
                        UUID.randomUUID().toString(),
                        Timestamp.of(DateTime.now()),
                        brandRef
                ));
            }
        }

        if (content instanceof Item) {
            Item item = (Item) content;

            ContainerRef containerRef = item.getContainerRef();
            ItemRef ref = item.toRef();
            ItemSummary summary = item.toSummary();

            SeriesRef seriesRef = null;

            if (content instanceof Episode) {
                Episode episode = (Episode) content;
                seriesRef = episode.getSeriesRef();
            }

            org.atlasapi.content.v2.model.udt.ItemRef itemRef = itemRefTranslator.serialize(ref);
            org.atlasapi.content.v2.model.udt.ItemSummary itemSummary = itemSummaryTranslator.serialize(summary);

            if (containerRef != null) {
                result.add(accessor.addItemRefsToContainer(
                        containerRef.getId().longValue(),
                        ImmutableSet.of(itemRef)
                ));

                if (seriesRef == null) {
                    result.add(accessor.addItemSummariesToContainer(
                            containerRef.getId().longValue(),
                            ImmutableSet.of(itemSummary)
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
                        ImmutableSet.of(itemRef)
                ));

                result.add(accessor.addItemSummariesToContainer(
                        seriesRef.getId().longValue(),
                        ImmutableSet.of(itemSummary)
                ));

                messages.add(new ResourceUpdatedMessage(
                        UUID.randomUUID().toString(),
                        Timestamp.of(DateTime.now()),
                        seriesRef
                ));
            }
        }

        return result;
    }

    @Override
    public void writeBroadcast(ItemRef item, Optional<ContainerRef> containerRef,
            Optional<SeriesRef> seriesRef, Broadcast broadcast) {
        throw new UnsupportedOperationException("herp derp");
    }

    @Override
    public ListenableFuture<Resolved<Content>> resolveIds(Iterable<Id> ids) {
        List<ListenableFuture<Content>> futures = StreamSupport.stream(
                ids.spliterator(),
                false
        )
                .map(Id::longValue)
                .map(accessor::getContent)
                .map(contentFuture -> Futures.transform(
                        contentFuture,
                        (org.atlasapi.content.v2.model.Content content) -> content != null ? translator.deserialize(content) : null
                ))
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

    private void ensureId(Content content) {
        if (content.getId() == null) {
            content.setId(Id.valueOf(idGenerator.generateRaw()));
        }
    }
}
