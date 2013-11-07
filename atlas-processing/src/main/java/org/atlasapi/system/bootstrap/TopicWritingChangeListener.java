package org.atlasapi.system.bootstrap;

import java.util.concurrent.ThreadPoolExecutor;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.topic.Topic;
import org.atlasapi.media.topic.TopicStore;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class TopicWritingChangeListener extends AbstractMultiThreadedChangeListener<Topic> {

    private final TopicStore topicStore;
    private Function<Alias, Alias> cleanNamespaces = new Function<Alias, Alias>() {
        @Override
        public Alias apply(@Nullable Alias input) {
            return new Alias(clean(input.getNamespace()), input.getValue());
        }
    };

    public TopicWritingChangeListener(int concurrencyLevel, TopicStore topicStore) {
        super(concurrencyLevel);
        this.topicStore = topicStore;
    }

    public TopicWritingChangeListener(ThreadPoolExecutor executor, TopicStore topicStore) {
        super(executor);
        this.topicStore = topicStore;
    }

    @Override
    protected void onChange(Topic topic) {
        if (topic.getNamespace().equals("magpie")) {
            return;
        }
        topic.setAliases(ImmutableSet.<Alias>builder()
            .add(alias(topic))
            .addAll(Iterables.transform(topic.getAliases(), cleanNamespaces))
            .build());
        topicStore.writeTopic(topic);
    }

    private Alias alias(Topic topic) {
        return new Alias(namespace(topic), topic.getValue());
    }

    private String namespace(Topic topic) {
        return clean(topic.getNamespace());
    }

    private String clean(String ns) {
        if (ns.equals("dbpedia")) {
            ns = "uri";
        }
        return ns;
    }
}
