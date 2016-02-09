package org.atlasapi.system.bootstrap.workers;

import java.util.Set;

import org.atlasapi.entity.ResourceRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.EquivalenceAssertionMessage;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage.AdjacentRef;

import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.Timestamp;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.joda.time.DateTime;

public class ContentEquivalenceAssertionLegacyMessageSerializer
        extends
        LegacyMessageSerializer<ContentEquivalenceAssertionMessage, EquivalenceAssertionMessage> {

    public ContentEquivalenceAssertionLegacyMessageSerializer() {
        super(ContentEquivalenceAssertionMessage.class);
    }

    @Override
    protected EquivalenceAssertionMessage transform(ContentEquivalenceAssertionMessage leg) {
        String mid = leg.getMessageId();
        Timestamp ts = leg.getTimestamp();
        ResourceRef subj = resourceRef(
                leg.getEntityId(),
                leg.getEntitySource(),
                leg.getEntityType(),
                leg.getTimestamp()
        );
        Set<ResourceRef> adjacents = toResourceRef(leg);
        Set<Publisher> srcs = ImmutableSet.copyOf(Iterables.transform(
                leg.getSources(),
                Publisher.FROM_KEY
        ));
        return new EquivalenceAssertionMessage(mid, ts, subj, adjacents, srcs);
    }

    protected Set<ResourceRef> toResourceRef(ContentEquivalenceAssertionMessage leg) {
        if (leg.getAdjacent() == null || leg.getAdjacent().isEmpty()) {
            return ImmutableSet.of();
        }
        DateTime madeUpUpdatedTime = new DateTime(leg.getTimestamp().millis(), DateTimeZones.UTC);
        ImmutableSet.Builder<ResourceRef> resourceRefs = ImmutableSet.builder();
        for (AdjacentRef adjacentRef : leg.getAdjacent()) {
            resourceRefs.add(toResourceRef(
                    idCodec.decode(adjacentRef.getId()).longValue(),
                    Publisher.fromKey(adjacentRef.getSource()).requireValue(),
                    adjacentRef.getType(), madeUpUpdatedTime
            ));
        }
        return resourceRefs.build();
    }

}
