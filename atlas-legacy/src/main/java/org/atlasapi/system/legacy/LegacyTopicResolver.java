package org.atlasapi.system.legacy;

import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.topic.TopicQueryResolver;
import org.atlasapi.persistence.topic.TopicStore;
import org.atlasapi.topic.Topic;
import org.atlasapi.topic.TopicResolver;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;


public class LegacyTopicResolver implements TopicResolver {

    private final TopicStore topicStore;
    private final TopicQueryResolver topicResolver;
    private final LegacyTopicTransformer transformer;

    public LegacyTopicResolver(TopicQueryResolver topicResolver, TopicStore topicStore) {
        this.topicResolver = topicResolver;
        this.topicStore = topicStore;
        this.transformer = new LegacyTopicTransformer();
    }

    @Override
    public ListenableFuture<Resolved<Topic>> resolveIds(Iterable<Id> ids) {
        Iterable<Long> longIds = Iterables.transform(ids, Id.toLongValue());
        Iterable<org.atlasapi.media.entity.Topic> topics = topicResolver.topicsForIds(longIds);
        Iterable<Topic> transformed = Iterables.transform(topics, transformer);
        return Futures.immediateFuture(Resolved.valueOf(transformed));
    }

    @Override
    public OptionalMap<Alias, Topic> resolveAliases(Iterable<Alias> aliases, final Publisher source) {
        return ImmutableOptionalMap.copyOf(Maps.toMap(aliases,
            new Function<Alias, Optional<Topic>>() {
                @Override
                public Optional<Topic> apply(Alias input) {
                    Maybe<org.atlasapi.media.entity.Topic> topic = resolve(source, input);
                    return topic.hasValue() ? Optional.of(transformer.apply(topic.valueOrNull()))
                                            : Optional.<Topic>absent();
                }

                private Maybe<org.atlasapi.media.entity.Topic> resolve(final Publisher source, Alias input) {
                    return topicStore.topicFor(source, input.getNamespace(), input.getValue());
                }
            }
        ));
    }

    public Iterable<Topic> resolveAll() {
        return Iterables.transform(
                topicResolver.topicsFor(ContentQuery.MATCHES_EVERYTHING),
                transformer
        );
    }

}
