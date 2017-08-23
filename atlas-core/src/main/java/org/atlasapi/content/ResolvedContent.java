package org.atlasapi.content;

public class ResolvedContent {

    private Content content;

    private ResolvedContent(Builder builder) {
        content = builder.content;
    }

    public Content getContent() {
        return content;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private Content content;

        private Builder() {
        }

        public Builder withContent(Content content) {
            this.content = content;
            return this;
        }

        public ResolvedContent build() {
            return new ResolvedContent(this);
        }
    }
}
