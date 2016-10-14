package org.atlasapi.topic;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.collect.OptionalMap;

import com.google.common.util.concurrent.ListenableFuture;

public abstract class ForwardingTopicStore implements TopicStore {

    protected ForwardingTopicStore() {
    }

    protected abstract TopicStore delegate();

    @Override
    public ListenableFuture<Resolved<Topic>> resolveIds(Iterable<Id> ids) {
        return delegate().resolveIds(ids);
    }

    @Override
    public OptionalMap<Alias, Topic> resolveAliases(Iterable<Alias> aliases, Publisher source) {
        return delegate().resolveAliases(aliases, source);
    }

    @Override
    public WriteResult<Topic, Topic> writeTopic(Topic topic) {
        return delegate().writeTopic(topic);
    }

}
