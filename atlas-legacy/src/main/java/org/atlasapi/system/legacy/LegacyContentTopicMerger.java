package org.atlasapi.system.legacy;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.atlasapi.media.entity.TopicRef;

import com.google.common.base.Equivalence;
import com.google.common.collect.ImmutableSet;

public class LegacyContentTopicMerger {

    public static class TopicRefEquivalator extends Equivalence<TopicRef> {
        @Override
        protected boolean doEquivalent(TopicRef a, TopicRef b) {
            return ((a.getTopic() == b.getTopic())
                    && (a.getPublisher() == b.getPublisher()));
        }

        @Override
        protected int doHash(TopicRef topicRef) {
            return Objects.hash(topicRef.getTopic());
        }
    }

    private TopicRefEquivalator equivalator = new TopicRefEquivalator();

    public List<TopicRef> mergeTags(Collection<TopicRef>... topicCollections) {

        ImmutableSet.Builder<Equivalence.Wrapper<TopicRef>> setBuilder = new ImmutableSet.Builder<>();

        setBuilder.addAll(
                Arrays.asList(topicCollections).stream()
                        .flatMap(topicCollection -> topicCollection.stream())
                        .map(equivalator::wrap)
                        .collect(Collectors.toList()));

        return setBuilder.build().stream()
                .map(Equivalence.Wrapper::get)
                .collect(Collectors.toList());
    }
}
