package org.atlasapi.content;

import java.util.Set;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

import com.google.common.collect.ImmutableSet;

import static java.util.Optional.ofNullable;

public class Film extends Item {

    private String websiteUrl = null;
    private Set<Subtitles> subtitles = ImmutableSet.of();
    private Set<ReleaseDate> releaseDates = ImmutableSet.of();

    public Film(String uri, String curie, Publisher publisher) {
        super(uri, curie, publisher);
        setSpecialization(Specialization.FILM);
    }

    public Film(Id id, Publisher source) {
        super(id, source);
    }

    public Film() {
        setSpecialization(Specialization.FILM);
    }

    public void setWebsiteUrl(String websiteUrl) {
        this.websiteUrl = websiteUrl;
    }

    @FieldName("website_url")
    public String getWebsiteUrl() {
        return websiteUrl;
    }

    @FieldName("subtitles")
    public Set<Subtitles> getSubtitles() {
        return subtitles;
    }

    public void setSubtitles(Iterable<Subtitles> subtitles) {
        this.subtitles = ImmutableSet.copyOf(subtitles);
    }

    @FieldName("release_dates")
    public Set<ReleaseDate> getReleaseDates() {
        return releaseDates;
    }

    public void setReleaseDates(Iterable<ReleaseDate> releaseDates) {
        this.releaseDates = ImmutableSet.copyOf(releaseDates);
    }

    @Override
    public FilmRef toRef() {
        return new FilmRef(
                getId(),
                getSource(),
                SortKey.keyFrom(this),
                getThisOrChildLastUpdated()
        );
    }

    public static Film copyTo(Film from, Film to) {
        Item.copyTo(from, to);
        to.websiteUrl = from.websiteUrl;
        to.subtitles = from.subtitles;
        to.releaseDates = from.releaseDates;
        return to;
    }

    @Override public <T extends Described> T copyTo(T to) {
        if (to instanceof Film) {
            copyTo(this, (Film) to);
            return to;
        }
        return super.copyTo(to);
    }
    @Override public <T extends Described> T copyToPreferNonNull(T to) {
        if (to instanceof Film) {
            copyToPreferNonNull(this, (Film) to);
            return to;
        }
        return super.copyToPreferNonNull(to);
    }

    public static Film copyToPreferNonNull(Film from, Film to) {
        Item.copyToPreferNonNull(from, to);
        to.websiteUrl = ofNullable(from.websiteUrl).orElse(to.websiteUrl);
        to.subtitles = from.subtitles == null || from.subtitles.isEmpty() ? to.subtitles : from.subtitles;
        to.releaseDates = from.releaseDates == null || from.releaseDates.isEmpty() ? to.releaseDates : from.releaseDates;
        return to;
    }

    @Override public Film copy() {
        return copyTo(this, new Film());
    }

    @Override
    public Film createNew() {
        return new Film();
    }

    public <V> V accept(ItemVisitor<V> visitor) {
        return visitor.visit(this);
    }

}
