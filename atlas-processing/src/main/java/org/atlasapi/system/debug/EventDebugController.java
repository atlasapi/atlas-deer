package org.atlasapi.system.debug;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.event.Event;
import org.atlasapi.event.EventResolver;
import org.atlasapi.eventV2.EventV2;
import org.atlasapi.eventV2.EventV2Resolver;
import org.atlasapi.system.legacy.LegacyPersistenceModule;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.api.client.json.JsonGenerator;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import org.joda.time.DateTime;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import static com.google.common.base.Preconditions.checkNotNull;

@Controller
public class EventDebugController {

    private final LegacyPersistenceModule legacyPersistenceModule;
    private final AtlasPersistenceModule atlasPersistenceModule;
    private final NumberToShortStringCodec lowercaseDecoder;
    private final Gson gson;

    public EventDebugController(
            LegacyPersistenceModule legacyPersistenceModule,
            AtlasPersistenceModule atlasPersistenceModule) {
        this.legacyPersistenceModule = checkNotNull(legacyPersistenceModule);
        this.atlasPersistenceModule = checkNotNull(atlasPersistenceModule);
        this.lowercaseDecoder = SubstitutionTableNumberCodec.lowerCaseOnly();
        this.gson = new GsonBuilder().registerTypeAdapter(
                DateTime.class,
                (JsonSerializer<DateTime>) (src, typeOfSrc, context) ->
                        new JsonPrimitive(src.toString())
        )
                .create();
    }

    @RequestMapping("/system/debug/event/{id}/legacy")
    public void printLegacyEvent(@PathVariable("id") String id, HttpServletResponse response)
            throws Exception {
        Id decodedId = Id.valueOf(lowercaseDecoder.decode(id));

        EventResolver legacyEventResolver = legacyPersistenceModule.legacyEventResolver();
        ListenableFuture<Resolved<Event>> future = legacyEventResolver.resolveIds(
                ImmutableList.of(decodedId));

        Resolved<Event> resolved = Futures.get(future, Exception.class);
        Event event = Iterables.getOnlyElement(resolved.getResources());
        gson.toJson(event, response.getWriter());
    }

    @RequestMapping("/system/debug/event/{id}")
    public void printEvent(@PathVariable("id") String id, HttpServletResponse response)
            throws Exception {
        Id decodedId = Id.valueOf(lowercaseDecoder.decode(id));

        EventResolver eventResolver = atlasPersistenceModule.eventResolver();
        ListenableFuture<Resolved<Event>> future = eventResolver.resolveIds(
                ImmutableList.of(decodedId));

        Resolved<Event> resolved = Futures.get(future, Exception.class);
        Event event = Iterables.getOnlyElement(resolved.getResources());
        gson.toJson(event, response.getWriter());
    }

    @RequestMapping("/system/debug/eventv2/{id}")
    public void printEventV2(@PathVariable("id") String id, HttpServletResponse response)
            throws Exception {
        Id decodedId = Id.valueOf(lowercaseDecoder.decode(id));

        EventV2Resolver eventResolver = atlasPersistenceModule.eventV2Resolver();
        ListenableFuture<Resolved<EventV2>> future = eventResolver.resolveIds(
                ImmutableList.of(decodedId));

        Resolved<EventV2> resolved = Futures.get(future, Exception.class);
        EventV2 event = Iterables.getOnlyElement(resolved.getResources());
        gson.toJson(event, response.getWriter());
    }
}
