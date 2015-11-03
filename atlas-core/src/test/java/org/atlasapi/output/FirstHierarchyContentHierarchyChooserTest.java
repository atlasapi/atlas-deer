package org.atlasapi.output;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;

import org.atlasapi.content.Brand;
import org.atlasapi.content.Container;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.Series;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

@RunWith(MockitoJUnitRunner.class)
public class FirstHierarchyContentHierarchyChooserTest {

    @Mock
    private EquivalentSetContentHierarchyChooser fallback;
    
    @InjectMocks
    private FirstHierarchyContentHierarchyChooser underTest;
    
    @Test
    public void testFirstBrandWithHierarchyChosen() {
        Brand mostPrecedentWithoutSeries = brand(123L);
        
        Brand lessPrecedentWithSeries = brand(456L);
        lessPrecedentWithSeries.setSeriesRefs(ImmutableSet.of(seriesRef(4L)));
        
        Optional<Container> chosen = underTest.chooseBestHierarchy(ImmutableList.of(mostPrecedentWithoutSeries, lessPrecedentWithSeries));
        
        assertThat(chosen.get(), is(lessPrecedentWithSeries));
    }
    
    @Test
    public void testFallbackUsedIfNoHiearchyFound() {
        Brand mostPrecedentWithoutSeries = brand(123L);
        Brand lessPrecedentWithoutSeries = brand(456L);
        
        ImmutableList<Container> containers = ImmutableList.of(mostPrecedentWithoutSeries, lessPrecedentWithoutSeries);
        underTest.chooseBestHierarchy(containers);
        
        verify(fallback).chooseBestHierarchy(containers);
    }
    
    @Test
    public void testFirstSeriesWithChildrenChosen() {
        Series series = series(123L);
        series.setItemRefs(ImmutableSet.of(itemRef(789L)));
        Brand lessPrecedentBrand = brand(456L);
        
        Optional<Container> chosen = underTest.chooseBestHierarchy(ImmutableList.of(series, lessPrecedentBrand));
        
        assertThat(chosen.get(), is(series));
    }

    private Brand brand(long id) {
        return new Brand(Id.valueOf(id), Publisher.METABROADCAST);
    }
    
    private Series series(long id) {
        return new Series(Id.valueOf(id), Publisher.METABROADCAST);
    }
    
    private SeriesRef seriesRef(long id) {
        return new SeriesRef(Id.valueOf(id), Publisher.METABROADCAST, "", 1, DateTime.now());
    }
    
    private ItemRef itemRef(long id) {
        return new ItemRef(Id.valueOf(id), Publisher.METABROADCAST, "1", DateTime.now());
    }
    
}
