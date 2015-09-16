package org.atlasapi.system.bootstrap;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.event.Event;
import org.atlasapi.event.EventResolver;
import org.atlasapi.event.EventWriter;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

@Controller
public class EventBootstrapController {

    private final EventResolver resolver;
    private final EventWriter writer;
    private final NumberToShortStringCodec idCodec;

    public EventBootstrapController(EventResolver resolver, EventWriter writer) {
        this(resolver, writer, SubstitutionTableNumberCodec.lowerCaseOnly());
    }

    @VisibleForTesting
    EventBootstrapController(EventResolver resolver, EventWriter writer,
            NumberToShortStringCodec idCodec) {
        this.resolver = checkNotNull(resolver);
        this.writer = checkNotNull(writer);
        this.idCodec = checkNotNull(idCodec);
    }

    @RequestMapping(value="/system/bootstrap/event/{id}", method= RequestMethod.POST)
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
}
