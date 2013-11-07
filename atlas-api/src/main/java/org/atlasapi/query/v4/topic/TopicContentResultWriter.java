package org.atlasapi.query.v4.topic;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.atlasapi.content.Content;
import org.atlasapi.output.ContextualResultWriter;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.ContextualQueryResult;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.topic.Topic;

public class TopicContentResultWriter implements ContextualResultWriter<Topic, Content> {

    private final EntityWriter<Topic> topicWriter;
    private final EntityListWriter<Content> contentWriter;

    public TopicContentResultWriter(EntityWriter<Topic> topicWriter, EntityListWriter<Content> contentWriter) {
        this.topicWriter = checkNotNull(topicWriter);
        this.contentWriter = checkNotNull(contentWriter);
    }

    @Override
    public void write(ContextualQueryResult<Topic, Content> result, ResponseWriter writer)
        throws IOException {
        writer.startResponse();
        writeResult(result, writer);
        writer.finishResponse();
    }

    private void writeResult(ContextualQueryResult<Topic, Content> result, ResponseWriter writer)
        throws IOException {

        OutputContext ctxt = outputContext(result.getContext());

        writer.writeObject(topicWriter, result.getContextResult().getOnlyResource(), ctxt);
        writer.writeList(contentWriter, result.getResourceResult().getResources(), ctxt);
        
    }

    private OutputContext outputContext(QueryContext queryContext) {
        return new OutputContext(
            queryContext.getAnnotations(),
            queryContext.getApplicationSources());
    }

}
