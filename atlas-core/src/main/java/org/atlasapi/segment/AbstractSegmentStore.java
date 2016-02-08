package org.atlasapi.segment;

import java.util.Set;
import java.util.UUID;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.ResourceUpdatedMessage;

import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessagingException;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.Timestamp;

import com.google.common.base.Equivalence;
import com.google.common.base.Optional;
import com.google.common.primitives.Longs;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

abstract public class AbstractSegmentStore implements SegmentStore {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final IdGenerator idGen;
    private final Equivalence<? super Segment> equivalence;
    private final MessageSender<ResourceUpdatedMessage> sender;
    private final Clock clock;

    public AbstractSegmentStore(IdGenerator idGen, Equivalence<? super Segment> equivalence,
            MessageSender<ResourceUpdatedMessage> sender, Clock clock) {
        this.idGen = checkNotNull(idGen);
        this.equivalence = checkNotNull(equivalence);
        this.sender = checkNotNull(sender);
        this.clock = checkNotNull(clock);
    }

    @Override
    public WriteResult<Segment, Segment> writeSegment(Segment segment) {
        checkNotNull(segment, "Cannot persist a null segment");
        checkNotNull(segment.getSource(), "Cannot persist a segment without a publisher");

        Segment previous = getPreviousSegment(segment);
        if (previous != null) {
            if (equivalence.equivalent(segment, previous)) {
                return WriteResult.<Segment, Segment>unwritten(segment)
                        .withPrevious(previous)
                        .build();
            }
            segment.setId(previous.getId());
            segment.setFirstSeen(previous.getFirstSeen());
        }

        DateTime now = clock.now();
        if (segment.getFirstSeen() == null) {
            segment.setFirstSeen(now);
        }
        segment.setLastUpdated(now);
        doWrite(ensureId(segment), previous);
        WriteResult<Segment, Segment> result = WriteResult.<Segment, Segment>written(segment)
                .withPrevious(previous)
                .build();
        if (result.written()) {
            writeMessage(result);
        }
        return result;
    }

    private void writeMessage(WriteResult<Segment, Segment> result) {
        ResourceUpdatedMessage msg = createEntityUpdatedMessage(result);
        try {
            Id resourceId = msg.getUpdatedResource().getId();
            sender.sendMessage(msg, Longs.toByteArray(resourceId.longValue()));
        } catch (MessagingException e) {
            log.error("Failed to send resource update message [{}] - {}",
                    msg.getUpdatedResource().toString(), e.toString()
            );
        }
    }

    private <T extends Segment> ResourceUpdatedMessage createEntityUpdatedMessage(
            WriteResult<T, T> result) {

        return new ResourceUpdatedMessage(
                UUID.randomUUID().toString(),
                Timestamp.of(result.getWriteTime().getMillis()),
                result.getResource().toRef()
        );

    }

    protected abstract void doWrite(Segment segment, Segment previous);

    private Segment ensureId(Segment segment) {
        segment.setId(segment.getId() != null ? segment.getId()
                                              : Id.valueOf(idGen.generateRaw()));
        return segment;
    }

    private Segment getPreviousSegment(Segment segment) {
        return resolvePrevious(segment.getId(), segment.getSource(), segment.getAliases()).orNull();
    }

    protected abstract Optional<Segment> resolvePrevious(Id id, Publisher source,
            Set<Alias> aliases);
}
