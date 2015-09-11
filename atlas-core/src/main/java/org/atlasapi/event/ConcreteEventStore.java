package org.atlasapi.event;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.UUID;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.Timestamp;

public class ConcreteEventStore implements EventStore {

    private static final Event NO_PREVIOUS = null;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Clock clock;
    private final IdGenerator idGenerator;
    private final EventHasher eventHasher;
    private final MessageSender<ResourceUpdatedMessage> sender;
    private final EventPersistenceStore persistenceStore;

    protected ConcreteEventStore(Clock clock, IdGenerator idGenerator, EventHasher eventHasher,
            MessageSender<ResourceUpdatedMessage> sender, EventPersistenceStore persistenceStore) {
        this.clock = checkNotNull(clock);
        this.idGenerator = checkNotNull(idGenerator);
        this.sender = checkNotNull(sender);
        this.eventHasher = checkNotNull(eventHasher);
        this.persistenceStore = checkNotNull(persistenceStore);
    }

    @Override
    public ListenableFuture<Resolved<Event>> resolveIds(Iterable<Id> ids) {
        return persistenceStore.resolveIds(ids);
    }

    @Override
    public WriteResult<Event, Event> write(Event event) throws WriteException {
        checkNotNull(event, "cannot write null event");
        checkNotNull(event.getSource(), "cannot write event without source");

        WriteResult<Event, Event> result = writeInternal(event);

        if (result.written()) {
            sendResourceUpdatedMessage(result);
        }

        return result;
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
            sender.sendMessage(message);
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
        ConcreteEventStore build();
    }

    private static class Builder implements ClockStep, IdGeneratorStep, EventHasherStep, SenderStep,
            PersistenceStoreStep, BuildStep {

        private Clock clock;
        private IdGenerator idGenerator;
        private EventHasher eventHasher;
        private MessageSender<ResourceUpdatedMessage> sender;
        private EventPersistenceStore persistenceStore;

        private Builder() {}

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

        @Override
        public ConcreteEventStore build() {
            return new ConcreteEventStore(
                    this.clock,
                    this.idGenerator,
                    this.eventHasher,
                    this.sender,
                    this.persistenceStore
            );
        }
    }
}
