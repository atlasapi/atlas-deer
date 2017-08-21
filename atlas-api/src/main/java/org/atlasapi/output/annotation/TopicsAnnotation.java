package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.Map;

import org.atlasapi.content.Content;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.content.Tag;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.topic.Topic;

import com.google.common.collect.Maps;

public class TopicsAnnotation extends OutputAnnotation<Content, ResolvedContent> {

    private final EntityWriter<Topic> topicWriter;

    public TopicsAnnotation(EntityWriter<Topic> topicListWriter) {
        super();
        this.topicWriter = topicListWriter;
    }

//    private Iterable<Topic> resolve(List<Id> topicIds) throws IOException {
//        if (topicIds.isEmpty()) { // don't even ask (the resolver)
//            return ImmutableList.of();
//        }
//        //TODO: more specific exception, probably, please?
//        return Futures.get(topicResolver.resolveIds(topicIds),
//                1, TimeUnit.MINUTES, IOException.class
//        ).getResources();
//    }

    @Override
    public void write(ResolvedContent entity, FieldWriter writer, OutputContext ctxt) throws IOException {
//        List<Tag> tags = entity.getTags();
//        Iterable<Topic> topics = resolve(Lists.transform(tags, Tag::getTopic));
        final Map<Id, Topic> topicsMap = Maps.uniqueIndex(entity.getTopics(), Identified::getId);

        writer.writeList(
                new EntityListWriter<Tag>() {

                    @Override
                    public void write(Tag entity, FieldWriter writer, OutputContext ctxt)
                            throws IOException {
                        writer.writeObject(topicWriter, topicsMap.get(entity.getTopic()), ctxt);
                        writer.writeField("supervised", entity.isSupervised());
                        writer.writeField("weighting", entity.getWeighting());
                        writer.writeField("relationship", entity.getRelationship());
                        writer.writeField("offset", entity.getOffset());
                    }

                    @Override
                    public String listName() {
                        return "tags";
                    }

                    @Override
                    public String fieldName(Tag entity) {
                    return "tag";
                }

                },
                entity.getContent().getTags(),
                ctxt
        );
    }

}
