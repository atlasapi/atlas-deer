package org.atlasapi.content;

import com.google.common.base.Strings;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

import javax.annotation.Nullable;

import static java.util.Optional.ofNullable;

public class Series extends Container {

    private Integer seriesNumber;
    private Integer totalEpisodes;
    private BrandRef brandRef;

    public Series() {
    }

    public Series(String uri, String curie, Publisher publisher) {
        super(uri, curie, publisher);
    }

    public Series(Id id, Publisher source) {
        super(id, source);
    }

    public ContainerSummary toSummary() {
        return ContainerSummary.from(this);
    }

    public Series withSeriesNumber(Integer seriesNumber) {
        this.seriesNumber = seriesNumber;
        return this;
    }

    @Nullable
    @FieldName("series_number")
    public Integer getSeriesNumber() {
        return seriesNumber;
    }

    public void setBrand(Brand brand) {
        this.brandRef = brand.toRef();
    }

    public void setBrandRef(@Nullable BrandRef brandRef) {
        this.brandRef = brandRef;
    }

    @Nullable
    @FieldName("brand_ref")
    public BrandRef getBrandRef() {
        return this.brandRef;
    }

    public static Series copyTo(Series from, Series to) {
        Container.copyTo(from, to);
        to.seriesNumber = from.seriesNumber;
        to.totalEpisodes = from.totalEpisodes;
        to.brandRef = from.brandRef;
        return to;
    }

    @Override public <T extends Described> T copyTo(T to) {
        if (to instanceof Series) {
            copyTo(this, (Series) to);
            return to;
        }
        return super.copyTo(to);
    }

    @Override public Series copy() {
        return copyTo(this, new Series());
    }

    @Override public <T extends Described> T copyToPreferNonNull(T to) {
        if (to instanceof Series) {
            copyToPreferNonNull(this, (Series) to);
            return to;
        }
        return super.copyToPreferNonNull(to);
    }

    public static Series copyToPreferNonNull(Series from, Series to) {
        Container.copyToPreferNonNull(from, to);
        to.seriesNumber = ofNullable(from.seriesNumber).orElse(to.seriesNumber);
        to.totalEpisodes = ofNullable(from.totalEpisodes).orElse(to.totalEpisodes);
        to.brandRef = ofNullable(from.brandRef).orElse(to.brandRef);
        return to;
    }

    @Override
    public Series createNew() {
        return new Series();
    }

    public SeriesRef toRef() {
        return new SeriesRef(getId(), getSource(), Strings.nullToEmpty(this.getTitle()),
                this.seriesNumber, getThisOrChildLastUpdated(), getYear(), getCertificates()
        );
    }

    public void setTotalEpisodes(Integer totalEpisodes) {
        this.totalEpisodes = totalEpisodes;
    }

    @FieldName("total_episodes")
    public Integer getTotalEpisodes() {
        return totalEpisodes;
    }

    @Override
    public <V> V accept(ContainerVisitor<V> visitor) {
        return visitor.visit(this);
    }

    @Override
    public <V> V accept(ContentVisitor<V> visitor) {
        return accept((ContainerVisitor<V>) visitor);
    }

}
