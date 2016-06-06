package org.atlasapi.system.legacy;

import java.util.Set;
import org.atlasapi.media.entity.TopicRef;

public interface GenreToTagMapper {
    Set<TopicRef> mapGenresToTopicRefs(Set<String> genres);
}
