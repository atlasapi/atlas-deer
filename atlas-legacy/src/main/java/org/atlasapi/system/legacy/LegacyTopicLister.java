package org.atlasapi.system.legacy;

import org.atlasapi.entity.ResourceLister;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.mongo.MongoTopicStore;
import org.atlasapi.topic.Topic;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;

public class LegacyTopicLister implements ResourceLister<Topic> {

    private final MongoTopicStore topicStore;
	private final Function<org.atlasapi.media.entity.Topic, Topic> transformer;

    public LegacyTopicLister(MongoTopicStore topicStore) {
        this.topicStore = topicStore;
        this.transformer = new LegacyTopicTransformer();
    }

    @Override
    public FluentIterable<Topic> list() {
        return FluentIterable.from(topicStore.all()).transform(transformer);
    }

    @Override
    public FluentIterable<Topic> list(Iterable<Publisher> sources) {
        throw new UnsupportedOperationException();
    }

}
