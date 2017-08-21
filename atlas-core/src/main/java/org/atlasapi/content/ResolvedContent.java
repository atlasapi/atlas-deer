package org.atlasapi.content;

import org.atlasapi.equivalence.Equivalable;
import org.atlasapi.topic.Topic;

import java.util.List;

public class ResolvedContent extends Content implements Equivalable<ResolvedContent> {

    private final Content content;
    private final List<Topic> topics;
    private final List<Clip> clips;

    private ResolvedContent(Builder builder) {
        this.content = builder.content;
        this.topics = builder.topics;
        this.clips = builder.clips;
    }

    public Content getContent() {
        return content;
    }

    public List<Topic> getTopics() {
        return topics;
    }

    public List<Clip> getClips() {
        return clips;
    }

    @Override
    public <V> V accept(ContentVisitor<V> visitor) {
        return content.accept(visitor);
    }

    @Override
    public ContentRef toRef() {
        return content.toRef();
    }

    @Override
    public Described copy() {
        return content.copy();
    }

    public static Builder resolvedContentBuilder() {
        return new Builder();
    }

    public static final class Builder {

        private Content content;
        private List<Topic> topics;
        private List<Clip> clips;

        private Builder() {
        }

        public Builder withContent(Content content) {
            this.content = content;
            return this;
        }

        public Builder withTopics(List<Topic> topics) {
            this.topics = topics;
            return this;
        }

        public Builder withClips(List<Clip> clips) {
            this.clips = clips;
            return this;
        }

        public ResolvedContent build() {
            return new ResolvedContent(this);
        }
    }
}
