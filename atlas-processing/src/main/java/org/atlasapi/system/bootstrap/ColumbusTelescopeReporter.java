package org.atlasapi.system.bootstrap;

import java.time.LocalDateTime;

import org.atlasapi.content.Content;

import com.metabroadcast.columbus.telescope.api.EntityState;
import com.metabroadcast.columbus.telescope.api.Environment;
import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.columbus.telescope.api.Process;
import com.metabroadcast.columbus.telescope.client.TelescopeClientImpl;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.media.MimeType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ColumbusTelescopeReporter {

    private final Logger log = LoggerFactory.getLogger(ColumbusTelescopeReporter.class);
    private final SubstitutionTableNumberCodec codec;
    private final ObjectMapper mapper;
    private final TelescopeClientImpl telescopeClient;
    private final Process process;

    private ColumbusTelescopeReporter(
            TelescopeClientImpl telescopeClient,
            String reportingEnvironment,
            ObjectMapper mapper
    ) {
        this.mapper = checkNotNull(mapper);
        this.codec = SubstitutionTableNumberCodec.lowerCaseOnly();
        this.telescopeClient = checkNotNull(telescopeClient);

        this.process = Process.create(
                "Atlas Owl to Atlas Deer Content migration",
                "atlas-owl-to-atlas-deer-content-migration",
                (checkNotNull(reportingEnvironment).equals("prod")) ?
                Environment.PRODUCTION :
                Environment.STAGE
        );
    }

    public static ColumbusTelescopeReporter create(
            TelescopeClientImpl telescopeClient,
            String reportingEnvironment,
            ObjectMapper mapper
    ) {
        return new ColumbusTelescopeReporter(telescopeClient, reportingEnvironment, mapper);
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

        telescopeClient.createEvent(event);
    }

    private Event createEvent(
            EntityState entityState,
            Event.Status eventStatus
    ) {
        return Event.builder()
                .withProcess(process)
                .withType(Event.Type.MIGRATION)
                .withStatus(eventStatus)
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