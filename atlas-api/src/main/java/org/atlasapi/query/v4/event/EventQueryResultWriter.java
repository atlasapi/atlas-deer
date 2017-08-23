package org.atlasapi.query.v4.event;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.content.ResolvedContent;
import org.atlasapi.event.Event;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryResult;

import com.google.common.collect.FluentIterable;

import static com.google.common.base.Preconditions.checkNotNull;

public class EventQueryResultWriter extends QueryResultWriter<Event> {

    private final EntityListWriter<ResolvedContent> eventListWriter;

    public EventQueryResultWriter(
            EntityListWriter<ResolvedContent> eventListWriter,
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
            Iterable<ResolvedContent> events = StreamSupport.stream(result.getResources().spliterator(), false)
                    .map(ResolvedContent::wrap)
                    .collect(Collectors.toList());

            writer.writeList(eventListWriter, events, ctxt);
        } else {
            writer.writeObject(eventListWriter, ResolvedContent.wrap(result.getOnlyResource()), ctxt);
        }
    }
}

