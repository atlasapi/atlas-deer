package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

import org.joda.time.DateTime;

public class FilmRef extends ItemRef {

    public FilmRef(Id id, Publisher source, String sortKey, DateTime updated) {
        super(id, source, sortKey, updated);
    }

    @FieldName("content_type")
    @Override
    public ContentType getContentType() {
        return ContentType.FILM;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof FilmRef) {
            FilmRef other = (FilmRef) that;
            return id.equals(other.id)
                    && source.equals(other.source)
                    && getContentType().equals(other.getContentType());
        }
        return false;
    }

}
