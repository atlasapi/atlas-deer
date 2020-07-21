package org.atlasapi.system.bootstrap;

import java.io.IOException;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.event.Event;
import org.atlasapi.event.EventResolver;
import org.atlasapi.event.EventWriter;

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

    private final EventResolver eventResolver;
    private final EventWriter eventWriter;
    private final NumberToShortStringCodec idCodec;

    public EventBootstrapController(EventResolver eventResolver, EventWriter eventsV2writer) {
        this(eventResolver, SubstitutionTableNumberCodec.lowerCaseOnly(), eventsV2writer);
    }

    @VisibleForTesting
    EventBootstrapController(
            EventResolver eventResolver,
            NumberToShortStringCodec idCodec,
            EventWriter eventWriter
    ) {
        this.idCodec = checkNotNull(idCodec);
        this.eventWriter = checkNotNull(eventWriter);
        this.eventResolver = checkNotNull(eventResolver);
    }
    @RequestMapping(value = "/system/bootstrap/event/{id}", method = RequestMethod.POST)
    public void bootstrapEvent(@PathVariable("id") String encodedId, HttpServletResponse resp)
            throws IOException {
        Id id = Id.valueOf(idCodec.decode(encodedId).longValue());
        executeBootstrap(resp, id);
    }

    @RequestMapping(value = "/system/bootstrap/event/numeric/{id}", method = RequestMethod.POST)
    public void bootstrapEvent(@PathVariable("id") Long numericId, HttpServletResponse resp)
            throws IOException {
        Id id = Id.valueOf(numericId);
        executeBootstrap(resp, id);
    }

    private void executeBootstrap(HttpServletResponse resp, Id id) throws IOException {
        ListenableFuture<Resolved<Event>> future = eventResolver.resolveIds(ImmutableList.of(id));

        Resolved<Event> resolved = Futures.get(future, IOException.class);
        if (resolved.getResources().isEmpty()) {
            resp.sendError(HttpStatus.NOT_FOUND.value());
            return;
        }
        for (Event event : resolved.getResources()) {
            try {
                eventWriter.write(event);
            } catch (WriteException e) {
                Throwables.propagate(e);
            }
        }
        resp.setStatus(HttpStatus.OK.value());
        resp.setContentLength(0);
    }
}
