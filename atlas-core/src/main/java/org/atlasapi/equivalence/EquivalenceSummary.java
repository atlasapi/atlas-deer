package org.atlasapi.equivalence;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.atlasapi.content.ContentRef;
import org.atlasapi.content.Described;
import org.atlasapi.content.Item;
import org.atlasapi.content.Series;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiable;
import org.atlasapi.entity.Sourced;
import org.atlasapi.media.entity.Publisher;

import javax.annotation.Nullable;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Records computed candidates and resultant equivalents of the equivalence process. This intended
 * for use in other equivalence settings, i.e. the results of a Brand's children can influence the
 * result of the Brand itself, or vice-versa.
 * <p>
 * Any other use, such as resolving content, is probably not a good idea.
 */
public class EquivalenceSummary {

    public static class Ref implements Identifiable, Sourced {

        public static Ref valueOf(Described content) {
            Publisher publisher = content.getSource();
            Id id = content.getId();
            Id parentId = null;
            if (content instanceof Item) {
                parentId = idOrNull(((Item) content).getContainerRef());
            }
            if (content instanceof Series) {
                parentId = idOrNull(((Series) content).getBrandRef());
            }
            return new Ref(id, publisher, parentId);
        }

        public static final Function<Described, Ref> FROM_CONTENT = new Function<Described, Ref>() {

            @Override
            public Ref apply(@Nullable Described input) {
                return Ref.valueOf(input);
            }
        };

        protected static Id idOrNull(@Nullable ContentRef parent) {
            return parent == null ? null : parent.getId();
        }

        private final Id id;
        private final Publisher publisher;
        private final Optional<Id> parentId;

        public Ref(long id, Publisher publisher, @Nullable Id parentId) {
            this(Id.valueOf(id), publisher, parentId);
        }

        public Ref(Id id, Publisher publisher, @Nullable Id parentId) {
            this.id = checkNotNull(id);
            this.publisher = checkNotNull(publisher);
            this.parentId = Optional.fromNullable(parentId);
        }

        public Id getId() {
            return this.id;
        }

        public Publisher getSource() {
            return this.publisher;
        }

        public Optional<Id> getParentId() {
            return this.parentId;
        }

        @Override
        public boolean equals(Object that) {
            if (this == that) {
                return true;
            }
            if (that instanceof Ref) {
                Ref other = (Ref) that;
                return id.equals(other.id)
                        && Objects.equal(parentId, other.parentId);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                    .add("id", id)
                    .add("publisher", publisher)
                    .add("parent", parentId)
                    .toString();
        }
    }

    private final Id subject;
    private final Optional<Id> parent;
    private final ImmutableList<Id> candidates;
    private final ImmutableMap<Publisher, Ref> equivalents;

    public EquivalenceSummary(long subject, long parent, Iterable<Id> candidates,
            Map<Publisher, Ref> equivalents) {
        this(Id.valueOf(subject), Id.valueOf(parent), candidates, equivalents);
    }

    public EquivalenceSummary(Id subject, Iterable<Id> candidates,
            Map<Publisher, Ref> equivalents) {
        this(checkNotNull(subject), null, candidates, equivalents);
    }

    public EquivalenceSummary(Id subject, @Nullable Id parent, Iterable<Id> candidates,
            Map<Publisher, Ref> equivalents) {
        this.subject = checkNotNull(subject);
        this.parent = Optional.fromNullable(parent);
        this.candidates = ImmutableList.copyOf(candidates);
        this.equivalents = ImmutableMap.copyOf(equivalents);
    }

    public Id getSubject() {
        return this.subject;
    }

    public Optional<Id> getParent() {
        return this.parent;
    }

    public ImmutableList<Id> getCandidates() {
        return this.candidates;
    }

    public ImmutableMap<Publisher, Ref> getEquivalents() {
        return this.equivalents;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof EquivalenceSummary) {
            EquivalenceSummary other = (EquivalenceSummary) that;
            return subject.equals(other.subject)
                    && Objects.equal(parent, other.parent)
                    && candidates.equals(other.candidates)
                    && equivalents.equals(other.equivalents);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(subject, parent, candidates, equivalents);
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper(getClass())
                .add("subject", subject)
                .add("parent", parent)
                .add("candidates", candidates)
                .add("equivalents", equivalents)
                .toString();
    }
}
