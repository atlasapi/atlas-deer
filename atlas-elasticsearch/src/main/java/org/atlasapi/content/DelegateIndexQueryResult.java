package org.atlasapi.content;

import java.util.LinkedList;
import java.util.List;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.ImmutableCollectors;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class is used when a content index is used as a delegate of another content index to
 * pass additional information between them without polluting {@link IndexQueryResult} with it
 */
public class DelegateIndexQueryResult {

    private final ImmutableList<Result> results;
    private final Long count;

    private DelegateIndexQueryResult(Iterable<Result> results, long totalResultCount) {
        this.results = ImmutableList.copyOf(results);
        this.count = totalResultCount;
    }

    public static Builder builder(long totalResultCount) {
        return new Builder(totalResultCount);
    }

    public Long getTotalCount() {
        return count;
    }

    public FluentIterable<Id> getIds() {
        return FluentIterable.from(
                results.stream()
                    .map(Result::getId)
                    .collect(ImmutableCollectors.toList())
        );
    }

    public FluentIterable<Result> getResults() {
        return FluentIterable.from(results);
    }

    public static class Builder {

        private final long totalResultCount;

        private List<Result> results = new LinkedList<>();

        private Builder(long totalResultCount) {
            this.totalResultCount = totalResultCount;
        }

        public Builder add(Id id, Id canonicalId, Publisher publisher) {
            results.add(Result.of(id, canonicalId, publisher));
            return this;
        }

        public DelegateIndexQueryResult build() {
            return new DelegateIndexQueryResult(results, totalResultCount);
        }
    }

    public static class Result {

        private final Id id;
        private final Id canonicalId;
        private final Publisher publisher;

        private Result(Id id, Id canonicalId, Publisher publisher) {
            this.id = checkNotNull(id);
            this.canonicalId = checkNotNull(canonicalId);
            this.publisher = checkNotNull(publisher);
        }

        public static Result of(Id id, Id canonicalId, Publisher publisher) {
            return new Result(id, canonicalId, publisher);
        }

        public Id getId() {
            return id;
        }

        public Id getCanonicalId() {
            return canonicalId;
        }

        public Publisher getPublisher() {
            return publisher;
        }
    }
}
