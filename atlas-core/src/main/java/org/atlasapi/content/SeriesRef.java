package org.atlasapi.content;

import java.util.List;
import java.util.Set;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;
import org.joda.time.DateTime;

import com.google.common.base.Objects.ToStringHelper;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;

public class SeriesRef extends ContainerRef implements Comparable<SeriesRef> {

    private static final Ordering<SeriesRef> NATURAL = Ordering.natural().reverse();
    
    private final String title;
    private final DateTime updated;
    private final Integer seriesNumber;
    private final Integer releaseYear;
    private final ImmutableSet<Certificate> certificates;
    
    public static List<SeriesRef> dedupeAndSort(Iterable<SeriesRef> seriesRefs) {
        return NATURAL.immutableSortedCopy(ImmutableSet.copyOf(seriesRefs));
    }
    
    public SeriesRef(Id id, Publisher source) {
        this(id, source, null, null, null, null, null);
    }

    public SeriesRef(Id id, Publisher source, String title, Integer seriesNumber, DateTime updated) {
        super(id, source);
        this.title = title;
        this.seriesNumber = seriesNumber;
        this.updated = updated;
        this.releaseYear = null;
        this.certificates = null;
    }
    
    public SeriesRef(Id id, Publisher source, String title, Integer seriesNumber, DateTime updated, Integer releaseYear, Iterable<Certificate> certs) {
        super(id, source);
        this.title =  title;
        this.seriesNumber = seriesNumber;
        this.updated = updated;
        this.releaseYear = releaseYear;
        this.certificates = certs != null ? ImmutableSet.copyOf(certs) : ImmutableSet.of();
    }
    
    @FieldName("content_type")
    @Override
    public ContentType getContentType() {
        return ContentType.SERIES;
    }

    @FieldName("title")
    public String getTitle() {
        return title;
    }
    
    @FieldName("updated")
    public DateTime getUpdated() {
        return updated;
    }
    
    @FieldName("series_number")
    public Integer getSeriesNumber() {
        return seriesNumber;
    }

    public ImmutableSet<Certificate> getCertificates() {
        return certificates;
    }

    public Integer getReleaseYear() {
        return releaseYear;
    }

    @Override
    public int compareTo(SeriesRef comparableTo) {
        if (seriesNumber != null && comparableTo.seriesNumber != null) {
            return seriesNumber.compareTo(comparableTo.getSeriesNumber());
        } else {
            return title.compareTo(comparableTo.title);
        }
    }
    
    @Override
    protected ToStringHelper toStringHelper() {
        return super.toStringHelper()
            .add("number", seriesNumber)
            .add("title", title)
            .add("updated", updated);
            
    }
    
    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof SeriesRef) {
            SeriesRef other = (SeriesRef) that;
            return id.equals(other.id);
        }
        return false;
    }

}
