package org.atlasapi.system.debug;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.event.Event;
import org.atlasapi.event.EventResolver;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

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

    private final EventResolver legacyEventResolver;
    private final AtlasPersistenceModule atlasPersistenceModule;
    private final NumberToShortStringCodec lowercaseDecoder;
    private final Gson gson;

    public EventDebugController(
            EventResolver legacyEventResolver,
            AtlasPersistenceModule atlasPersistenceModule) {
        this.legacyEventResolver = checkNotNull(legacyEventResolver);
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

        ListenableFuture<Resolved<Event>> future = legacyEventResolver.resolveIds(
                ImmutableList.of(decodedId));

        Resolved<Event> resolved = Futures.getChecked(future, Exception.class);
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

        Resolved<Event> resolved = Futures.getChecked(future, Exception.class);
        Event event = Iterables.getOnlyElement(resolved.getResources());
        gson.toJson(event, response.getWriter());
    }
}
