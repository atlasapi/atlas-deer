package org.atlasapi.topic;

import java.util.Set;
import java.util.UUID;

import javax.annotation.Nullable;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.ResourceUpdatedMessage;

import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.Timestamp;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Equivalence;
import com.google.common.base.Throwables;
import com.google.common.primitives.Longs;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractTopicStore implements TopicStore {

    private final IdGenerator idGenerator;
    private final Equivalence<? super Topic> equivalence;
    private final MessageSender<ResourceUpdatedMessage> sender;
    private final Clock clock;
    private final Meter calledMeter;
    private final Meter failureMeter;

    protected final Logger log = LoggerFactory.getLogger(getClass());

    public AbstractTopicStore(IdGenerator idGenerator, Equivalence<? super Topic> equivalence,
            MessageSender<ResourceUpdatedMessage> sender, Clock clock, MetricRegistry metricRegistry,
            String metricPrefix) {
        this.idGenerator = checkNotNull(idGenerator);
        this.equivalence = checkNotNull(equivalence);
        this.sender = checkNotNull(sender);
        this.clock = checkNotNull(clock);
        this.calledMeter = checkNotNull(metricRegistry).meter(checkNotNull(metricPrefix) + "meter.called");
        this.failureMeter = checkNotNull(metricRegistry).meter(checkNotNull(metricPrefix) + "meter.failure");
    }

    @Override
    public WriteResult<Topic, Topic> writeTopic(Topic topic) {
        calledMeter.mark();
        try {
            checkNotNull(topic, "write null topic");
            checkNotNull(topic.getSource(), "write unsourced topic");
        } catch(NullPointerException e) {
            failureMeter.mark();
            Throwables.propagate(e);
        }

        Topic previous = getPreviousTopic(topic);
        if (previous != null) {
            if (equivalence.equivalent(topic, previous)) {
                return WriteResult.<Topic, Topic>unwritten(topic)
                        .withPrevious(previous)
                        .build();
            }
            topic.setId(previous.getId());
            topic.setFirstSeen(previous.getFirstSeen());
        }

        DateTime now = clock.now();
        if (topic.getFirstSeen() == null) {
            topic.setFirstSeen(now);
        }
        topic.setLastUpdated(now);
        doWrite(ensureId(topic), previous);
        WriteResult<Topic, Topic> result = WriteResult.<Topic, Topic>written(topic)
                .withPrevious(previous)
                .build();
        if (result.written()) {
            writeMessage(result);
        }
        return result;
    }

    private void writeMessage(final WriteResult<Topic, Topic> result) {
        ResourceUpdatedMessage message = createEntityUpdatedMessage(result);
        try {
            Id resourceId = message.getUpdatedResource().getId();
            sender.sendMessage(message, Longs.toByteArray(resourceId.longValue()));
        } catch (Exception e) {
            log.error(message.getUpdatedResource().toString(), e);
        }
    }

    private <T extends Topic> ResourceUpdatedMessage createEntityUpdatedMessage(
            WriteResult<T, T> result) {
        return new ResourceUpdatedMessage(
                UUID.randomUUID().toString(),
                Timestamp.of(result.getWriteTime().getMillis()),
                result.getResource().toRef()
        );
    }

    private Topic ensureId(Topic topic) {
        topic.setId(topic.getId() != null ? topic.getId()
                                          : Id.valueOf(idGenerator.generateRaw()));
        return topic;
    }

    protected abstract void doWrite(Topic topic, Topic previous);

    private Topic getPreviousTopic(Topic topic) {
        return resolvePrevious(topic.getId(), topic.getSource(), topic.getAliases());
    }

    @Nullable
    protected abstract Topic resolvePrevious(@Nullable Id id, Publisher source, Set<Alias> aliases);

}
