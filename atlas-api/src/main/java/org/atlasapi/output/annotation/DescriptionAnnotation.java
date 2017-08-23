package org.atlasapi.output.annotation;

import java.io.IOException;

import com.sun.tools.javac.comp.Resolve;
import org.atlasapi.content.Described;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.SourceWriter;
import org.atlasapi.topic.Topic;

public class DescriptionAnnotation<T extends Described, R extends ResolvedContent> extends OutputAnnotation<T, R> {

    private final EntityWriter<Publisher> publisherWriter = SourceWriter.sourceWriter("source");

    @Override
    public void write(R entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeObject(publisherWriter, entity.getContent().getSource(), ctxt);
        if (entity.getDescribed() instanceof Topic) {
            Topic topic = (Topic) entity.getDescribed();
            writer.writeField("topic_type", topic.getType());
        }
        writer.writeField("title", entity.getContent().getTitle());
        writer.writeField("description", entity.getContent().getDescription());
        writer.writeField("image", entity.getContent().getImage());
        writer.writeField("thumbnail", entity.getContent().getThumbnail());
    }

}