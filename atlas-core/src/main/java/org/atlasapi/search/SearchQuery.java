package org.atlasapi.search;

import java.util.Set;

import org.atlasapi.content.Specialization;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.query.Selection;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class SearchQuery {

    private final String term;
    private final Selection selection;
    private final Set<Specialization> includedSpecializations;
    private final Set<Publisher> includedPublishers;
    private final float titleWeighting;
    private final float broadcastWeighting;
    private final float catchupWeighting;
    private final String type;

    /**
     * Use a Builder
     */
    @Deprecated
    public SearchQuery(String term, Selection selection, Iterable<Publisher> includedPublishers,
            float titleWeighting, float broadcastWeighting, float availabilityWeighting) {
        this(
                term,
                selection,
                Sets.<Specialization>newHashSet(),
                includedPublishers,
                titleWeighting,
                broadcastWeighting,
                availabilityWeighting,
                null
        );
    }

    public SearchQuery(String term, Selection selection,
            Iterable<Specialization> includedSpecializations,
            Iterable<Publisher> includedPublishers,
            float titleWeighting, float broadcastWeighting, float availabilityWeighting,
            String type) {
        this.term = term;
        this.selection = selection;
        this.titleWeighting = titleWeighting;
        this.broadcastWeighting = broadcastWeighting;
        this.catchupWeighting = availabilityWeighting;
        this.type = type;
        this.includedSpecializations = ImmutableSet.copyOf(includedSpecializations);
        this.includedPublishers = ImmutableSet.copyOf(includedPublishers);
    }

    public String getTerm() {
        return term;
    }

    public Selection getSelection() {
        return selection;
    }

    public Set<Specialization> getIncludedSpecializations() {
        return includedSpecializations;
    }

    public Set<Publisher> getIncludedPublishers() {
        return includedPublishers;
    }

    public float getTitleWeighting() {
        return titleWeighting;
    }

    public float getBroadcastWeighting() {
        return broadcastWeighting;
    }

    public float getCatchupWeighting() {
        return catchupWeighting;
    }

    public String type() {
        return this.type;
    }

    public static final Builder builder(String query) {
        return new Builder(query);
    }

    public static class Builder {

        private final String query;
        private Selection selection = Selection.ALL;
        private Set<Specialization> specializations = ImmutableSet.of();
        private Set<Publisher> publishers = ImmutableSet.of();
        private float title = 0;
        private float broadcast = 0;
        private float catchup = 0;
        private String type;

        public Builder(String query) {
            this.query = query;
        }

        public Builder withSelection(Selection selection) {
            this.selection = selection;
            return this;
        }

        public Builder withSpecializations(Iterable<Specialization> specializations) {
            this.specializations = ImmutableSet.copyOf(specializations);
            return this;
        }

        public Builder withPublishers(Iterable<Publisher> publishers) {
            this.publishers = ImmutableSet.copyOf(publishers);
            return this;
        }

        public Builder withTitleWeighting(float titleWeighting) {
            this.title = titleWeighting;
            return this;
        }

        public Builder withBroadcastWeighting(float broadcastWeighting) {
            this.broadcast = broadcastWeighting;
            return this;
        }

        public Builder withCatchupWeighting(float catchupWeighting) {
            this.catchup = catchupWeighting;
            return this;
        }

        public Builder withType(String type) {
            this.type = type;
            return this;
        }

        public SearchQuery build() {
            return new SearchQuery(query,
                    selection,
                    specializations,
                    publishers,
                    title,
                    broadcast,
                    catchup,
                    type
            );
        }
    }
}
