package org.atlasapi.content;

import org.atlasapi.hashing.Hashable;
import org.atlasapi.meta.annotations.FieldName;

import com.google.common.base.Objects;

public class Description implements Hashable {

    public static final Description EMPTY = new Description("", "", "", "");

    public static class Builder {

        private String title;
        private String synopsis;

        private String image;
        private String thumbnail;

        private Builder() {
        }

        private Builder(String t, String synopsis, String img, String thmb) {
            this.title = t;
            this.synopsis = synopsis;
            this.image = img;
            this.thumbnail = thmb;
        }

        public Builder withTitle(String title) {
            this.title = title;
            return this;
        }

        public Builder withSynopsis(String synopsis) {
            this.synopsis = synopsis;
            return this;
        }

        public Builder withImage(String image) {
            this.image = image;
            return this;
        }

        public Builder withThumbnail(String thumbnail) {
            this.thumbnail = thumbnail;
            return this;
        }

        public Description build() {
            return new Description(title, synopsis, image, thumbnail);
        }
    }

    public static final Builder description() {
        return new Builder();
    }

    public Description(String title, String synopsis, String image, String thumbnail) {
        this.title = title;
        this.synopsis = synopsis;
        this.image = image;
        this.thumbnail = thumbnail;
        this.hash = Objects.hashCode(title, synopsis, image, thumbnail);
    }

    private final String title;

    private final String synopsis;

    private final String image;
    private final String thumbnail;

    private final transient int hash;

    @FieldName("title")
    public String getTitle() {
        return this.title;
    }

    @FieldName("synopsis")
    public String getSynopsis() {
        return this.synopsis;
    }

    @FieldName("image")
    public String getImage() {
        return this.image;
    }

    @FieldName("thumbnail")
    public String getThumbnail() {
        return this.thumbnail;
    }

    public Builder copy() {
        return new Builder(title, synopsis, image, thumbnail);
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof Description) {
            Description other = (Description) that;
            return other.hash == this.hash;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return String.format("%s Description", title);
    }
}
