package org.atlasapi.output.annotation;

import com.google.common.primitives.Ints;
import org.atlasapi.content.Described;
import org.atlasapi.content.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.SourceWriter;
import org.atlasapi.topic.Topic;

import java.io.IOException;

public class DescriptionAnnotation<T extends Described> extends
        OutputAnnotation<T> {

    private final EntityWriter<Publisher> publisherWriter = SourceWriter.sourceWriter("source");

    @Override
    public void write(T entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeObject(publisherWriter, entity.getSource(), ctxt);
        if (entity instanceof Topic) {
            Topic topic = (Topic) entity;
            writer.writeField("topic_type", topic.getType());
        }
        writer.writeField("title", entity.getTitle());
        writer.writeField("description", entity.getDescription());
        writer.writeField("image", entity.getImage());
        writer.writeField("thumbnail", entity.getThumbnail());
        if (entity instanceof Item) {
            Item item = (Item) entity;
            writer.writeField(
                    "duration",
                    item.getDuration() == null
                            ? null
                            : Ints.saturatedCast(item.getDuration().getStandardSeconds())
            );
        }
    }

}