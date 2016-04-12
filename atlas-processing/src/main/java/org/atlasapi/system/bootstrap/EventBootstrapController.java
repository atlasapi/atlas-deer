package org.atlasapi.system.bootstrap;

import java.io.IOException;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
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

    private final EventV2Resolver v2Resolver;
    private final EventV2Writer eventV2Writer;
    private final NumberToShortStringCodec idCodec;

    public EventBootstrapController(EventV2Resolver v2Resolver, EventV2Writer eventsV2writer) {
        this(v2Resolver, SubstitutionTableNumberCodec.lowerCaseOnly(), eventsV2writer);
    }

    @VisibleForTesting
    EventBootstrapController(EventV2Resolver v2Resolver,
            NumberToShortStringCodec idCodec, EventV2Writer eventV2Writer) {
        this.idCodec = checkNotNull(idCodec);
        this.eventV2Writer = checkNotNull(eventV2Writer);
        this.v2Resolver = checkNotNull(v2Resolver);
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
