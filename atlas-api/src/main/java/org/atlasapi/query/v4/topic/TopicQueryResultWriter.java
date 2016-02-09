package org.atlasapi.query.v4.topic;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.topic.Topic;

import com.google.common.collect.FluentIterable;

public class TopicQueryResultWriter extends QueryResultWriter<Topic> {

    private final EntityListWriter<Topic> topicListWriter;

    public TopicQueryResultWriter(
            EntityListWriter<Topic> topicListWriter,
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
            FluentIterable<Topic> topics = result.getResources();
            writer.writeList(topicListWriter, topics, ctxt);
        } else {
            writer.writeObject(topicListWriter, result.getOnlyResource(), ctxt);
        }
    }
}
