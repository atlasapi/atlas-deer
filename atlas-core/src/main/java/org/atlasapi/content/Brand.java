/* Copyright 2009 British Broadcasting Corporation
   Copyright 2009 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.simple.BrandSummary;
import org.atlasapi.meta.annotations.FieldName;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

/**
 * 
 * @author Robert Chatley (robert@metabroadcast.com)
 * @author Chris Jackson
 */
public class Brand extends Container {
    
    private ImmutableList<SeriesRef> seriesRefs = ImmutableList.of();

    public Brand(String uri, String curie, Publisher publisher) {
		super(uri, curie, publisher);
	}
    
    public Brand(Id id, Publisher source) {
        super(id, source);
    }
    
    public Brand() { /* some legacy code still requires a default constructor */ }

    @FieldName("series_refs")
    public ImmutableList<SeriesRef> getSeriesRefs() {
        return seriesRefs ;
    }
    
    public void setSeriesRefs(Iterable<SeriesRef> seriesRefs) {
        this.seriesRefs = ImmutableList.copyOf(seriesRefs);
    }
    
    public ContainerSummary toSummary() {
        return new ContainerSummary(
                getClass().getSimpleName().toLowerCase(),
                getTitle(),
                getDescription(),
                null
        );
    }

	@Override
	public Container copy() {
	    Brand copy = new Brand();
	    Container.copyTo(this, copy);
	    return copy;
	}

    public static final Function<Brand, Brand> COPY = new Function<Brand, Brand>() {
        @Override
        public Brand apply(Brand input) {
            return (Brand) input.copy();
        }
    };
    
    public <V> V accept(ContainerVisitor<V> visitor) {
        return visitor.visit(this);
    }
    
    @Override
    public BrandRef toRef() {
        return new BrandRef(getId(), getPublisher());
    }
    
}
