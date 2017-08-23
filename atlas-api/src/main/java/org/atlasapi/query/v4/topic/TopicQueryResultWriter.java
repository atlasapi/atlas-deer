package org.atlasapi.query.v4.topic;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.content.ResolvedContent;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.topic.Topic;

import com.google.common.collect.FluentIterable;

public class TopicQueryResultWriter extends QueryResultWriter<Topic> {

    private final EntityListWriter<ResolvedContent> topicListWriter;

    public TopicQueryResultWriter(
            EntityListWriter<ResolvedContent> topicListWriter,
            EntityWriter<Object> licenseWriter,
            EntityWriter<HttpServletRequest> requestWriter
    ) {
        super(licenseWriter, requestWriter);
        this.topicListWriter = topicListWriter;
    }

    @Override
    protected void writeResult(QueryResult<Topic> result, ResponseWriter writer)
            throws IOException {

        OutputContext ctxt = OutputContext.valueOf(result.getContext());

        if (result.isListResult()) {
            Iterable<ResolvedContent> topics = StreamSupport.stream(result.getResources().spliterator(), false)
                    .map(ResolvedContent::wrap)
                    .collect(Collectors.toList());

            writer.writeList(topicListWriter, topics, ctxt);
        } else {
            writer.writeObject(topicListWriter, ResolvedContent.wrap(result.getOnlyResource()), ctxt);
        }
    }
}
