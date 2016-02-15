package org.atlasapi.eventV2;

import java.util.UUID;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.event.Event;
import org.atlasapi.event.EventHasher;
import org.atlasapi.event.EventPersistenceStore;
import org.atlasapi.event.EventRef;
import org.atlasapi.messaging.ResourceUpdatedMessage;

import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.Timestamp;

import com.google.common.base.Optional;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ConcreteEventV2Store implements EventV2Store {

    private static final EventV2 NO_PREVIOUS = null;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Clock clock;
    private final IdGenerator idGenerator;
    private final EventV2Hasher eventHasher;
    private final MessageSender<ResourceUpdatedMessage> sender;
    private final EventV2PersistenceStore persistenceStore;

    protected ConcreteEventV2Store(Clock clock, IdGenerator idGenerator, EventV2Hasher eventHasher,
            MessageSender<ResourceUpdatedMessage> sender, EventV2PersistenceStore persistenceStore) {
        this.clock = checkNotNull(clock);
        this.idGenerator = checkNotNull(idGenerator);
        this.sender = checkNotNull(sender);
        this.eventHasher = checkNotNull(eventHasher);
        this.persistenceStore = checkNotNull(persistenceStore);
    }

    @Override
    public ListenableFuture<Resolved<EventV2>> resolveIds(Iterable<Id> ids) {
        return persistenceStore.resolveIds(ids);
    }

    @Override
    public WriteResult<EventV2, EventV2> write(EventV2 event) throws WriteException {
        checkNotNull(event, "cannot write null event");
        checkNotNull(event.getSource(), "cannot write event without source");

        WriteResult<EventV2, EventV2> result = writeInternal(event);

        if (result.written()) {
            sendResourceUpdatedMessage(result);
        }

        return result;
    }

    private WriteResult<EventV2, EventV2> writeInternal(EventV2 event) throws WriteException {
        Optional<EventV2> previous = getPreviousEvent(event);
        if (previous.isPresent()) {
            return writeEventWithPrevious(event, previous.get());
        }
        return writeNewEvent(event);
    }

    private Optional<EventV2> getPreviousEvent(EventV2 event) {
        return persistenceStore.resolvePrevious(
                Optional.fromNullable(event.getId()), event.getSource(), event.getAliases()
        );
    }

    private WriteResult<EventV2, EventV2> writeEventWithPrevious(EventV2 event, EventV2 previous) {
        boolean written = false;
        if (hashChanged(event, previous)) {
            updateWithPrevious(event, previous);
            writeEvent(event, previous);
            written = true;
        }
        return WriteResult.<EventV2, EventV2>result(event, written)
                .withPrevious(previous)
                .build();
    }

    private WriteResult<EventV2, EventV2> writeNewEvent(EventV2 event) {
        updateTimes(event);
        writeEvent(event, NO_PREVIOUS);
        return WriteResult.<EventV2, EventV2>written(event).build();
    }

    private boolean hashChanged(EventV2 writing, EventV2 previous) {
        return !eventHasher.hash(writing).equals(eventHasher.hash(previous));
    }

    private void updateWithPrevious(EventV2 writing, EventV2 previous) {
        writing.setId(previous.getId());
        updateTimes(writing);
    }

    private void updateTimes(EventV2 event) {
        DateTime now = clock.now();
        event.setLastUpdated(now);
    }

    private void ensureId(EventV2 event) {
        if (event.getId() == null) {
            Id id = Id.valueOf(idGenerator.generateRaw());
            event.setId(id);
        }
    }

    private void writeEvent(EventV2 event, EventV2 previous) {
        ensureId(event);
        persistenceStore.write(event, previous);
    }

    private void sendResourceUpdatedMessage(WriteResult<EventV2, EventV2> result) {
        ResourceUpdatedMessage message = createEntityUpdatedMessages(result);
        try {
            Id resourceId = message.getUpdatedResource().getId();
            sender.sendMessage(message, Longs.toByteArray(resourceId.longValue()));
        } catch (Exception e) {
            log.error("Failed to send message " + message.getUpdatedResource().toString(), e);
        }
    }

    private ResourceUpdatedMessage createEntityUpdatedMessages(WriteResult<EventV2, EventV2> result) {
        EventV2 writtenResource = result.getResource();
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

        SenderStep withEventHasher(EventV2Hasher eventHasher);
    }

    public interface SenderStep {

        PersistenceStoreStep withSender(MessageSender<ResourceUpdatedMessage> sender);
    }

    public interface PersistenceStoreStep {

        BuildStep withPersistenceStore(EventV2PersistenceStore persistenceStore);
    }

    public interface BuildStep {

        ConcreteEventV2Store build();
    }

    private static class Builder implements ClockStep, IdGeneratorStep, EventHasherStep, SenderStep,
            PersistenceStoreStep, BuildStep {

        private Clock clock;
        private IdGenerator idGenerator;
        private EventV2Hasher eventHasher;
        private MessageSender<ResourceUpdatedMessage> sender;
        private EventV2PersistenceStore persistenceStore;

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
        public SenderStep withEventHasher(EventV2Hasher eventHasher) {
            this.eventHasher = eventHasher;
            return this;
        }

        @Override
        public PersistenceStoreStep withSender(MessageSender<ResourceUpdatedMessage> sender) {
            this.sender = sender;
            return this;
        }

        @Override
        public BuildStep withPersistenceStore(EventV2PersistenceStore persistenceStore) {
            this.persistenceStore = persistenceStore;
            return this;
        }

        @Override
        public ConcreteEventV2Store build() {
            return new ConcreteEventV2Store(
                    this.clock,
                    this.idGenerator,
                    this.eventHasher,
                    this.sender,
                    this.persistenceStore
            );
        }
    }

}
