package org.atlasapi.system.bootstrap;

import java.time.LocalDateTime;

import org.atlasapi.content.Content;

import com.metabroadcast.columbus.telescope.api.EntityState;
import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.columbus.telescope.client.IngestTelescopeClientImpl;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.media.MimeType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ColumbusTelescopeReporter {

    private final Logger log = LoggerFactory.getLogger(ColumbusTelescopeReporter.class);
    private final SubstitutionTableNumberCodec codec;
    private final ObjectMapper mapper;
    private final IngestTelescopeClientImpl telescopeClient;

    public ColumbusTelescopeReporter(
            IngestTelescopeClientImpl telescopeClient
    ) {
        this.codec = SubstitutionTableNumberCodec.lowerCaseOnly();
        this.mapper = new ObjectMapper();
        this.telescopeClient = checkNotNull(telescopeClient);
    }

    public void reportSuccessfulMigration(
            Content content
    ) {
        EntityState entityState = EntityState.builder()
                .withAtlasId(codec.encode(content.getId().toBigInteger()))
                .withRaw(getContentJsonString(content))
                .withRawMime(MimeType.APPLICATION_JSON.toString())
                .build();

        Event event = createEvent(entityState, Event.Status.SUCCESS);

        telescopeClient.createEvents(ImmutableList.of(event));
    }

    public void reportFailedMigration(
            Content content,
            String reason
    ) {
        EntityState entityState = EntityState.builder()
                .withAtlasId(codec.encode(content.getId().toBigInteger()))
                .withRaw(getContentJsonString(content))
                .withRawMime(MimeType.APPLICATION_JSON.toString())
                .withError(reason)
                .build();

        Event event = createEvent(entityState, Event.Status.FAILURE);

        telescopeClient.createEvents(ImmutableList.of(event));
    }

    private Event createEvent(
            EntityState entityState,
            Event.Status eventStatus
    ) {
        return Event.builder()
                    .withStatus(eventStatus)
                    .withType(Event.Type.MIGRATION)
                    .withEntityState(entityState)
                    .withTimestamp(LocalDateTime.now())
                    .build();
    }

    public String getContentJsonString(Content content) {
        try {
            return mapper.writeValueAsString(content);
        } catch (JsonProcessingException e) {
            log.error("Couldn't convert content to a JSON string.", e);
            return e.getMessage();
        }
    }
}