package org.atlasapi.content;

import com.google.common.collect.ImmutableSet;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Sameable;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

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

    @Override public Film copy() {
        return copyTo(this, new Film());
    }

    public <V> V accept(ItemVisitor<V> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean isSame(@Nullable Sameable other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        if (!super.isSame(other)) return false;
        Film film = (Film) other;
        return Objects.equals(websiteUrl, film.websiteUrl) &&
                Objects.equals(subtitles, film.subtitles) &&
                Objects.equals(releaseDates, film.releaseDates);
    }
}
