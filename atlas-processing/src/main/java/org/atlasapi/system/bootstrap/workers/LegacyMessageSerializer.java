package org.atlasapi.system.bootstrap.workers;

import java.util.Set;

import org.atlasapi.content.BrandRef;
import org.atlasapi.content.ClipRef;
import org.atlasapi.content.ContentType;
import org.atlasapi.content.EpisodeRef;
import org.atlasapi.content.FilmRef;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.content.SongRef;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage.AdjacentRef;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.atlasapi.serialization.json.JsonFactory;
import org.atlasapi.topic.TopicRef;
import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.queue.Message;
import com.metabroadcast.common.queue.MessageDeserializationException;
import com.metabroadcast.common.queue.MessageSerializer;
import com.metabroadcast.common.time.DateTimeZones;

public abstract class LegacyMessageSerializer<LM extends Message, M extends Message> implements MessageSerializer<M> {

    private final ObjectMapper mapper = JsonFactory.makeJsonMapper()
            .registerModule(new org.atlasapi.messaging.v3.JacksonMessageSerializer.MessagingModule());
    
    private Class<LM> legacyType;
    
    public LegacyMessageSerializer(Class<LM> legacyType) {
        this.legacyType = legacyType;
    }
    
    @Override
    public final byte[] serialize(M msg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final M deserialize(byte[] bytes) throws MessageDeserializationException {
        try {
            LM leg = mapper.readValue(bytes, legacyType);
            return transform(leg);
        } catch (Exception e) {
            throw new MessageDeserializationException(e);
        }
    }

    protected abstract M transform(LM leg);

    protected ResourceRef resourceRef(EntityUpdatedMessage leg) {
        final Long lid = Long.valueOf(leg.getEntityId());
        final Publisher src = Publisher.fromKey(leg.getEntitySource()).requireValue();
        final DateTime updated = new DateTime(leg.getTimestamp().millis(), DateTimeZones.UTC);
        String type = leg.getEntityType();
        Optional<ContentType> possContentType = ContentType.fromKey(type);
        if (possContentType.isPresent()) {
            return toResourceRef(lid, src, type, updated);
        } else {
            Publisher source = Publisher.fromKey(leg.getEntitySource()).requireValue();
            return new TopicRef(Id.valueOf(lid), source);
        }
    }

    protected Set<ResourceRef> toResourceRef(ContentEquivalenceAssertionMessage leg) {
        if (leg.getAdjacent() == null || leg.getAdjacent().isEmpty()) {
            return ImmutableSet.of();
        }
        DateTime madeUpUpdatedTime = new DateTime(leg.getTimestamp().millis(), DateTimeZones.UTC);
        ImmutableSet.Builder<ResourceRef> resourceRefs = ImmutableSet.builder();
        for (AdjacentRef adjacentRef : leg.getAdjacent()) {
            resourceRefs.add(toResourceRef(adjacentRef.getId(), adjacentRef.getSource(), 
                    adjacentRef.getType(), madeUpUpdatedTime));
        }
        return resourceRefs.build();
    }
    
    protected ResourceRef getSubject(ContentEquivalenceAssertionMessage leg) {
        DateTime madeUpUpdatedTime = new DateTime(leg.getTimestamp().millis(), DateTimeZones.UTC);
        Long lid = Long.valueOf(leg.getEntityId());
        final Publisher src = Publisher.fromKey(leg.getEntitySource()).requireValue();
        return toResourceRef(lid, src, leg.getEntityType(), madeUpUpdatedTime);
    }

    protected ResourceRef toResourceRef(final Long lid, final Publisher src,
            String type, final DateTime updated) {
        final Id rid = Id.valueOf(lid);
        ContentType contentType = ContentType.fromKey(type).get();
        return contentType.accept(new ContentType.Visitor<ResourceRef>(){
    
            @Override
            public BrandRef visitBrand(ContentType contentType) {
                return new BrandRef(rid, src);
            }
    
            @Override
            public ClipRef visitClip(ContentType contentType) {
                return new ClipRef(rid, src, "11", updated);
            }
    
            @Override
            public SongRef visitSong(ContentType contentType) {
                return new SongRef(rid, src, "11", updated);
            }
    
            @Override
            public FilmRef visitFilm(ContentType contentType) {
                return new FilmRef(rid, src, "11", updated);
            }
    
            @Override
            public EpisodeRef visitEpisode(ContentType contentType) {
                return new EpisodeRef(rid, src, "11", updated);
            }
    
            @Override
            public ItemRef visitItem(ContentType contentType) {
                return new ItemRef(rid, src, "11", updated);
            }
    
            @Override
            public SeriesRef visitSeries(ContentType contentType) {
                return new SeriesRef(rid, src);
            }
        });
    }

}
