package org.atlasapi.query.v4.event;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.FluentIterable;
import org.atlasapi.event.Event;
import org.atlasapi.output.*;
import org.atlasapi.query.common.QueryResult;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

public class EventQueryResultWriter extends QueryResultWriter<Event> {
    private final EntityListWriter<Event> eventListWriter;

    public EventQueryResultWriter(
            EntityListWriter<Event> eventListWriter,
            EntityWriter<Object> licenseWriter,
            EntityWriter<HttpServletRequest> requestWriter
    ) {
        super(licenseWriter, requestWriter);
        this.eventListWriter = checkNotNull(eventListWriter);
    }

    @Override
    protected void writeResult(QueryResult<Event> result, ResponseWriter writer)
            throws IOException {

        OutputContext ctxt = OutputContext.valueOf(result.getContext());

        if (result.isListResult()) {
            FluentIterable<Event> events = result.getResources();
            writer.writeList(eventListWriter, events, ctxt);
        } else {
            writer.writeObject(eventListWriter, result.getOnlyResource(), ctxt);
        }
    }
}

