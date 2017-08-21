package org.atlasapi.query.v4.topic;

import java.io.IOException;

import org.atlasapi.content.ResolvedContent;
import org.atlasapi.output.ContextualResultWriter;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.ContextualQueryResult;
import org.atlasapi.topic.Topic;

import static com.google.common.base.Preconditions.checkNotNull;

public class TopicContentResultWriter implements ContextualResultWriter<Topic, ResolvedContent> {

    private final EntityWriter<Topic> topicWriter;
    private final EntityListWriter<ResolvedContent> contentWriter;

    public TopicContentResultWriter(EntityWriter<Topic> topicWriter,
            EntityListWriter<ResolvedContent> contentWriter) {
        this.topicWriter = checkNotNull(topicWriter);
        this.contentWriter = checkNotNull(contentWriter);
    }

    @Override
    public void write(ContextualQueryResult<Topic, ResolvedContent> result, ResponseWriter writer)
            throws IOException {
        writer.startResponse();
        writeResult(result, writer);
        writer.finishResponse();
    }

    private void writeResult(ContextualQueryResult<Topic, ResolvedContent> result, ResponseWriter writer)
            throws IOException {

        OutputContext ctxt = OutputContext.valueOf(result.getContext());

        writer.writeObject(topicWriter, result.getContextResult().getOnlyResource(), ctxt);
        writer.writeList(contentWriter, result.getResourceResult().getResources(), ctxt);

    }
}
