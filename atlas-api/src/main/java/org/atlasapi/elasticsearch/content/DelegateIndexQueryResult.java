package org.atlasapi.elasticsearch.content;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import javax.annotation.Nullable;
import java.util.LinkedList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class is used when a content index is used as a delegate of another content index to pass
 * additional information between them without polluting {@link IndexQueryResult} with it
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
                        .collect(MoreCollectors.toImmutableList())
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


        public Builder add(
                Id id,
                float score,
                Id canonicalId,
                Publisher publisher,
                @Nullable String title
        ) {
            results.add(
                    Result.of(
                            id,
                            score,
                            canonicalId,
                            publisher,
                            title
                    )
            );
            return this;
        }

        public DelegateIndexQueryResult build() {
            return new DelegateIndexQueryResult(results, totalResultCount);
        }
    }

    public static class Result {

        private final Id id;
        private final float score;
        private final Id canonicalId;
        private final Publisher publisher;
        private final String title;

        private Result(
                Id id,
                float score,
                Id canonicalId,
                Publisher publisher,
                @Nullable String title
        ) {
            this.id = checkNotNull(id);
            this.score = score;
            this.canonicalId = checkNotNull(canonicalId);
            this.publisher = checkNotNull(publisher);
            this.title = title;
        }

        public static Result of(
                Id id,
                float score,
                Id canonicalId,
                Publisher publisher,
                @Nullable String title
        ) {
            return new Result(
                    id,
                    score,
                    canonicalId,
                    publisher,
                    title
            );
        }

        public Id getId() {
            return id;
        }

        public float getScore() {
            return score;
        }

        public Id getCanonicalId() {
            return canonicalId;
        }

        public Publisher getPublisher() {
            return publisher;
        }

        @Nullable
        public String getTitle() {
            return title;
        }
    }
}
