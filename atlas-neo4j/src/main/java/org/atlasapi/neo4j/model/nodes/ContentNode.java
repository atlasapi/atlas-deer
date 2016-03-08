package org.atlasapi.neo4j.model.nodes;

import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Relationship;

import static com.google.common.base.Preconditions.checkNotNull;

@NodeEntity(label = ContentNode.LABEL)
public class ContentNode extends EntityNode {

    public static final String LABEL = "ContentTest";
    public static final String EQUIVALENT_TO = "EQUIVALENT_TO";

    private Long contentId;
    private String data;

    @Relationship(type = EQUIVALENT_TO, direction = Relationship.OUTGOING)
    private Set<ContentNode> equivalentContent;

    public ContentNode() { }

    private ContentNode(Long contentId, String data, Set<ContentNode> equivalentContent) {
        this.contentId = checkNotNull(contentId);
        this.data = data;
        this.equivalentContent = equivalentContent;
    }

    public static Builder builder(Long contentId) {
        return new Builder(contentId);
    }

    public Long getContentId() {
        return contentId;
    }

    public String getData() {
        return data;
    }

    public Set<ContentNode> getEquivalentContent() {
        return equivalentContent != null
               ? ImmutableSet.copyOf(equivalentContent)
               : ImmutableSet.of();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ContentNode that = (ContentNode) o;
        return Objects.equals(contentId, that.contentId) &&
                Objects.equals(data, that.data) &&
                Objects.equals(equivalentContent, that.equivalentContent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), contentId, data, equivalentContent);
    }

    public static class Builder {

        private Long contentId;
        private String data;
        private Set<ContentNode> equivalentContent;

        private Builder(Long contentId) {
            this.contentId = contentId;
        }

        public Builder withData(String data) {
            this.data = data;
            return this;
        }

        public Builder withEquivalents(Iterable<ContentNode> equivalentContent) {
            this.equivalentContent = Sets.newHashSet(equivalentContent);
            return this;
        }

        public ContentNode build() {
            return new ContentNode(contentId, data, equivalentContent);
        }
    }
}
