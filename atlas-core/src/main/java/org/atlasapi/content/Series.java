package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.metabroadcast.common.time.DateTimeZones;

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

	public Series toSummary() {
	   Series summary = new Series(this.getCanonicalUri(), this.getCurie(), this.publisher);
       summary.setTitle(this.getTitle());
       summary.setDescription(this.getDescription());
       summary.withSeriesNumber(seriesNumber);
       summary.setLastUpdated(this.getLastUpdated());
       summary.setThumbnail(this.getThumbnail());
       summary.setImage(this.getImage());
       return summary;
	}

	public Series withSeriesNumber(Integer seriesNumber) {
		this.seriesNumber = seriesNumber;
		return this;
	}

	public Integer getSeriesNumber() {
		return seriesNumber;
	}
	
	public void setBrand(Brand brand) {
	    this.brandRef = brand.toRef();
	}

    public void setBrandRef(BrandRef brandRef) {
        this.brandRef = brandRef;
    }
	
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
        return new SeriesRef(this.getId(), getPublisher(), Strings.nullToEmpty(this.getTitle()), 
                this.seriesNumber, new DateTime(DateTimeZones.UTC));
    }
    
    public void setTotalEpisodes(Integer totalEpisodes) {
        this.totalEpisodes = totalEpisodes;
    }
    
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
