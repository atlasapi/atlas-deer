package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

import com.google.common.base.Function;
import com.google.common.base.Strings;

public class Series extends Container {
	
	private Integer seriesNumber;
	private Integer totalEpisodes;
	private BrandRef brandRef;
	
	public Series() {}
	
	public Series(String uri, String curie, Publisher publisher) {
		 super(uri, curie, publisher);
	}
	
    public Series(Id id, Publisher source) {
        super(id, source);
    }

	public ContainerSummary toSummary() {
       return new ContainerSummary(
               getClass().getSimpleName().toLowerCase(),
               getTitle(),
               getDescription(),
               seriesNumber
       );
	}

	public Series withSeriesNumber(Integer seriesNumber) {
		this.seriesNumber = seriesNumber;
		return this;
	}

	@FieldName("series_number")
	public Integer getSeriesNumber() {
		return seriesNumber;
	}
	
	public void setBrand(Brand brand) {
	    this.brandRef = brand.toRef();
	}

    public void setBrandRef(BrandRef brandRef) {
        this.brandRef = brandRef;
    }
	
    @FieldName("brand_ref")
	public BrandRef getBrandRef() {
	    return this.brandRef;
	}
	
	@Override
	public Container copy() {
	    Series copy = new Series();
	    Container.copyTo(this, copy);
	    copy.seriesNumber = seriesNumber;
	    return copy;
	}
	
	public final static Function<Series, Series> COPY = new Function<Series, Series>() {
        @Override
        public Series apply(Series input) {
            return (Series) input.copy();
        }
    };
    
    public SeriesRef toRef() {
        return new SeriesRef(getId(), getSource(), Strings.nullToEmpty(this.getTitle()),
                this.seriesNumber, getThisOrChildLastUpdated(), getYear(), getCertificates());
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
