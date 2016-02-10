package org.atlasapi.topic;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.IdResolver;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.collect.OptionalMap;

public interface TopicResolver extends IdResolver<Topic> {

    OptionalMap<Alias, Topic> resolveAliases(Iterable<Alias> aliases, Publisher source);

}
