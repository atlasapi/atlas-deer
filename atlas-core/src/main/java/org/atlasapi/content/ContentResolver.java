package org.atlasapi.content;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.IdResolver;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.collect.OptionalMap;

public interface ContentResolver extends IdResolver<Content> {

    OptionalMap<Alias, Content> resolveAliases(Iterable<Alias> aliases, Publisher source);

}
