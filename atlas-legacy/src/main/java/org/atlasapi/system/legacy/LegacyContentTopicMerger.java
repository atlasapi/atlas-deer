package org.atlasapi.system.legacy;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.media.entity.TopicRef;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Equivalence;

public class LegacyContentTopicMerger {
    private final TopicRefEquivalator equivalator = new TopicRefEquivalator();

    public List<TopicRef> mergeTags(Collection<TopicRef>... topicCollections) {

        Set<Equivalence.Wrapper<TopicRef>> dedupedTopics =
                Arrays.asList(topicCollections).stream()
                        .flatMap(topicCollection -> topicCollection.stream())
                        .map(equivalator::wrap)
                        .collect(MoreCollectors.toImmutableSet());

        return dedupedTopics.stream()
                .map(Equivalence.Wrapper::get)
                .collect(Collectors.toList());
    }

    public static class TopicRefEquivalator extends Equivalence<TopicRef> {
        @Override
        protected boolean doEquivalent(TopicRef a, TopicRef b) {
            return Objects.equals(a.getTopic(), b.getTopic())
                    && Objects.equals(a.getPublisher(), b.getPublisher());
        }

        @Override
        protected int doHash(TopicRef topicRef) {
            return topicRef.hashCode();
        }
    }
}
