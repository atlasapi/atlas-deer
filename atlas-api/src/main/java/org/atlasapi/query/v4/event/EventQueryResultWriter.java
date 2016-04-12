package org.atlasapi.query.v4.event;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.eventV2.EventV2;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryResult;

import com.google.common.collect.FluentIterable;

import static com.google.common.base.Preconditions.checkNotNull;

public class EventQueryResultWriter extends QueryResultWriter<EventV2> {

    private final EntityListWriter<EventV2> eventListWriter;

    public EventQueryResultWriter(
            EntityListWriter<EventV2> eventListWriter,
            EntityWriter<Object> licenseWriter,
            EntityWriter<HttpServletRequest> requestWriter
    ) {
        super(licenseWriter, requestWriter);
        this.eventListWriter = checkNotNull(eventListWriter);
    }

    @Override
    protected void writeResult(QueryResult<EventV2> result, ResponseWriter writer)
            throws IOException {

        OutputContext ctxt = OutputContext.valueOf(result.getContext());

        if (result.isListResult()) {
            FluentIterable<EventV2> events = result.getResources();
            writer.writeList(eventListWriter, events, ctxt);
        } else {
            writer.writeObject(eventListWriter, result.getOnlyResource(), ctxt);
        }
    }
}

