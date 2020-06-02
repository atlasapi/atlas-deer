package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

import org.joda.time.Duration;

import static java.util.Optional.ofNullable;

public class Song extends Item {

    private String isrc;

    public Song() {
        setMediaType(MediaType.AUDIO);
        setSpecialization(Specialization.MUSIC);
    }

    public Song(String uri, String curie, Publisher publisher) {
        super(uri, curie, publisher);
        setMediaType(MediaType.AUDIO);
        setSpecialization(Specialization.MUSIC);
    }

    public Song(Id id, Publisher source) {
        super(id, source);
        setMediaType(MediaType.AUDIO);
        setSpecialization(Specialization.MUSIC);
    }

    public void setIsrc(String isrc) {
        this.isrc = isrc;
    }

    @FieldName("isrc")
    public String getIsrc() {
        return isrc;
    }

    @Override
    public SongRef toRef() {
        return new SongRef(
                getId(),
                getSource(),
                SortKey.keyFrom(this),
                getThisOrChildLastUpdated()
        );
    }

    public static Song copyTo(Song from, Song to) {
        Item.copyTo(from, to);
        to.isrc = from.isrc;
        return to;
    }

    @Override public <T extends Described> T copyTo(T to) {
        if (to instanceof Song) {
            copyTo(this, (Song) to);
            return to;
        }
        return super.copyTo(to);
    }

    @Override public <T extends Described> T copyToPreferNonNull(T to) {
        if (to instanceof Song) {
            copyToPreferNonNull(this, (Song) to);
            return to;
        }
        return super.copyToPreferNonNull(to);
    }

    public static Song copyToPreferNonNull(Song from, Song to) {
        Item.copyToPreferNonNull(from, to);
        to.isrc = ofNullable(from.isrc).orElse(to.isrc);
        return to;
    }

    @Override public Song copy() {
        return copyTo(this, new Song());
    }

    @Override
    public Song createNew() {
        return new Song();
    }

    public <V> V accept(ItemVisitor<V> visitor) {
        return visitor.visit(this);
    }

}
