package org.atlasapi.event;

import java.util.UUID;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.messaging.ResourceUpdatedMessage;

import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.Timestamp;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ConcreteEventStore implements EventStore {

    private static final Event NO_PREVIOUS = null;

    private static final String METER_CALLED = "meter.called";
    private static final String METER_FAILURE = "meter.failure";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Clock clock;
    private final IdGenerator idGenerator;
    private final EventHasher eventHasher;
    private final MessageSender<ResourceUpdatedMessage> sender;
    private final EventPersistenceStore persistenceStore;

    private final MetricRegistry metricRegistry;
    private final String write;

    protected ConcreteEventStore(
            Clock clock,
            IdGenerator idGenerator,
            EventHasher eventHasher,
            MessageSender<ResourceUpdatedMessage> sender,
            EventPersistenceStore persistenceStore,
            MetricRegistry metricRegistry,
            String metricPrefix
    ) {
        this.clock = checkNotNull(clock);
        this.idGenerator = checkNotNull(idGenerator);
        this.sender = checkNotNull(sender);
        this.eventHasher = checkNotNull(eventHasher);
        this.persistenceStore = checkNotNull(persistenceStore);

        this.metricRegistry = metricRegistry;
        write = metricPrefix + "write.";
    }

    @Override
    public ListenableFuture<Resolved<Event>> resolveIds(Iterable<Id> ids) {
        return persistenceStore.resolveIds(ids);
    }

    @Override
    public WriteResult<Event, Event> write(Event event) throws WriteException {

        metricRegistry.meter(write + METER_CALLED).mark();
        try {
            checkNotNull(event, "cannot write null event");
            checkNotNull(event.getSource(), "cannot write event without source");
        } catch (NullPointerException e) {
            metricRegistry.meter(write + METER_FAILURE).mark();
            Throwables.propagate(e);
        }

        try {

            WriteResult<Event, Event> result = writeInternal(event);

            if (result.written()) {
                sendResourceUpdatedMessage(result);
            }

            return result;

        } catch (WriteException | RuntimeException e) {
            metricRegistry.meter(write + METER_FAILURE).mark();
            throw e;
        }
    }

    private WriteResult<Event, Event> writeInternal(Event event) throws WriteException {
        Optional<Event> previous = getPreviousEvent(event);
        if (previous.isPresent()) {
            return writeEventWithPrevious(event, previous.get());
        }
        return writeNewEvent(event);
    }

    private Optional<Event> getPreviousEvent(Event event) {
        return persistenceStore.resolvePrevious(
                Optional.fromNullable(event.getId()), event.getSource(), event.getAliases()
        );
    }

    private WriteResult<Event, Event> writeEventWithPrevious(Event event, Event previous) {
        boolean written = false;
        if (hashChanged(event, previous)) {
            updateWithPrevious(event, previous);
            writeEvent(event, previous);
            written = true;
        }
        return WriteResult.<Event, Event>result(event, written)
                .withPrevious(previous)
                .build();
    }

    private WriteResult<Event, Event> writeNewEvent(Event event) {
        updateTimes(event);
        writeEvent(event, NO_PREVIOUS);
        return WriteResult.<Event, Event>written(event).build();
    }

    private boolean hashChanged(Event writing, Event previous) {
        return !eventHasher.hash(writing).equals(eventHasher.hash(previous));
    }

    private void updateWithPrevious(Event writing, Event previous) {
        writing.setId(previous.getId());
        updateTimes(writing);
    }

    private void updateTimes(Event event) {
        DateTime now = clock.now();
        event.setLastUpdated(now);
    }

    private void ensureId(Event event) {
        if (event.getId() == null) {
            Id id = Id.valueOf(idGenerator.generateRaw());
            event.setId(id);
        }
    }

    private void writeEvent(Event event, Event previous) {
        ensureId(event);
        persistenceStore.write(event, previous);
    }

    private void sendResourceUpdatedMessage(WriteResult<Event, Event> result) {
        ResourceUpdatedMessage message = createEntityUpdatedMessages(result);
        try {
            Id resourceId = message.getUpdatedResource().getId();
            sender.sendMessage(message, Longs.toByteArray(resourceId.longValue()));
        } catch (Exception e) {
            log.error("Failed to send message " + message.getUpdatedResource().toString(), e);
        }
    }

    private ResourceUpdatedMessage createEntityUpdatedMessages(WriteResult<Event, Event> result) {
        Event writtenResource = result.getResource();
        return new ResourceUpdatedMessage(
                UUID.randomUUID().toString(),
                Timestamp.of(clock.now().withZone(DateTimeZone.UTC)),
                new EventRef(writtenResource.getId(), writtenResource.getSource())
        );
    }

    public static ClockStep builder() {
        return new Builder();
    }

    public interface ClockStep {

        IdGeneratorStep withClock(Clock clock);
    }

    public interface IdGeneratorStep {

        EventHasherStep withIdGenerator(IdGenerator idGenerator);
    }

    public interface EventHasherStep {

        SenderStep withEventHasher(EventHasher eventHasher);
    }

    public interface SenderStep {

        PersistenceStoreStep withSender(MessageSender<ResourceUpdatedMessage> sender);
    }

    public interface PersistenceStoreStep {

        BuildStep withPersistenceStore(EventPersistenceStore persistenceStore);
    }

    public interface BuildStep {

        BuildStep withMetricRegistry(MetricRegistry metricRegistry);
        BuildStep withMetricPrefix(String metricPrefix);
        ConcreteEventStore build();
    }

    private static class Builder implements ClockStep, IdGeneratorStep, EventHasherStep, SenderStep,
            PersistenceStoreStep, BuildStep {

        private Clock clock;
        private IdGenerator idGenerator;
        private EventHasher eventHasher;
        private MessageSender<ResourceUpdatedMessage> sender;
        private EventPersistenceStore persistenceStore;
        private MetricRegistry metricRegistry;
        private String metricPrefix;

        private Builder() {
        }

        @Override
        public IdGeneratorStep withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        @Override
        public EventHasherStep withIdGenerator(IdGenerator idGenerator) {
            this.idGenerator = idGenerator;
            return this;
        }

        @Override
        public SenderStep withEventHasher(EventHasher eventHasher) {
            this.eventHasher = eventHasher;
            return this;
        }

        @Override
        public PersistenceStoreStep withSender(MessageSender<ResourceUpdatedMessage> sender) {
            this.sender = sender;
            return this;
        }

        @Override
        public BuildStep withPersistenceStore(EventPersistenceStore persistenceStore) {
            this.persistenceStore = persistenceStore;
            return this;
        }

        public BuildStep withMetricRegistry(MetricRegistry metricRegistry) {
            this.metricRegistry = metricRegistry;
            return this;
        }

        public BuildStep withMetricPrefix(String metricPrefix) {
            this.metricPrefix = metricPrefix;
            return this;
        }

        @Override
        public ConcreteEventStore build() {
            return new ConcreteEventStore(
                    this.clock,
                    this.idGenerator,
                    this.eventHasher,
                    this.sender,
                    this.persistenceStore,
                    this.metricRegistry,
                    this.metricPrefix
            );
        }
    }

}
