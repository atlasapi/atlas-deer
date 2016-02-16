package org.atlasapi.system.bootstrap;

import java.io.IOException;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.event.Event;
import org.atlasapi.event.EventResolver;
import org.atlasapi.event.EventWriter;
import org.atlasapi.eventV2.EventV2;
import org.atlasapi.eventV2.EventV2Resolver;
import org.atlasapi.eventV2.EventV2Writer;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import static com.google.common.base.Preconditions.checkNotNull;

@Controller
public class EventBootstrapController {

    private final EventResolver resolver;
    private final EventV2Resolver v2Resolver;
    private final EventWriter writer;
    private final EventV2Writer eventV2Writer;
    private final NumberToShortStringCodec idCodec;

    public EventBootstrapController(EventResolver resolver, EventV2Resolver v2Resolver, EventWriter writer, EventV2Writer eventsV2writer) {
        this(resolver, v2Resolver, writer, SubstitutionTableNumberCodec.lowerCaseOnly(), eventsV2writer);
    }

    @VisibleForTesting
    EventBootstrapController(EventResolver resolver, EventV2Resolver v2Resolver, EventWriter writer,
            NumberToShortStringCodec idCodec, EventV2Writer eventV2Writer) {
        this.resolver = checkNotNull(resolver);
        this.writer = checkNotNull(writer);
        this.idCodec = checkNotNull(idCodec);
        this.eventV2Writer = checkNotNull(eventV2Writer);
        this.v2Resolver = checkNotNull(v2Resolver);
    }

    @RequestMapping(value = "/system/bootstrap/event/{id}", method = RequestMethod.POST)
    public void bootstrapEvent(@PathVariable("id") String encodedId, HttpServletResponse resp)
            throws IOException {
        Id id = Id.valueOf(idCodec.decode(encodedId).longValue());
        ListenableFuture<Resolved<Event>> future = resolver.resolveIds(ImmutableList.of(id));
        Resolved<Event> resolved = Futures.get(future, IOException.class);
        if (resolved.getResources().isEmpty()) {
            resp.sendError(HttpStatus.NOT_FOUND.value());
            return;
        }
        for (Event event : resolved.getResources()) {
            try {
                writer.write(event);
            } catch (WriteException e) {
                Throwables.propagate(e);
            }
        }
        resp.setStatus(HttpStatus.OK.value());
        resp.setContentLength(0);
    }

    @RequestMapping(value = "/system/bootstrap/eventv2/{id}", method = RequestMethod.POST)
    public void bootstrapEventV2(@PathVariable("id") String encodedId, HttpServletResponse resp)
            throws IOException {
        Id id = Id.valueOf(idCodec.decode(encodedId).longValue());
        executeBootstrap(resp, id);
    }

    @RequestMapping(value = "/system/bootstrap/eventv2/numeric/{id}", method = RequestMethod.POST)
    public void bootstrapEventV2(@PathVariable("id") Long numericId, HttpServletResponse resp)
            throws IOException {
        Id id = Id.valueOf(numericId);
        executeBootstrap(resp, id);
    }

    private void executeBootstrap(HttpServletResponse resp, Id id) throws IOException {
        ListenableFuture<Resolved<EventV2>> future = v2Resolver.resolveIds(ImmutableList.of(id));

        Resolved<EventV2> resolved = Futures.get(future, IOException.class);
        if (resolved.getResources().isEmpty()) {
            resp.sendError(HttpStatus.NOT_FOUND.value());
            return;
        }
        for (EventV2 event : resolved.getResources()) {
            try {
                eventV2Writer.write(event);
            } catch (WriteException e) {
                Throwables.propagate(e);
            }
        }
        resp.setStatus(HttpStatus.OK.value());
        resp.setContentLength(0);
    }
}
