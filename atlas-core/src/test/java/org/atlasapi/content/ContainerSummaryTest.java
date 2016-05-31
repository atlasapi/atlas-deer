package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import org.junit.Test;

import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ContainerSummaryTest {

    @Test
    public void fromSeries() throws Exception {
        Series series = new Series(Id.valueOf(0L), Publisher.METABROADCAST);
        series.setTitle("title");
        series.setDescription("description");
        series.withSeriesNumber(5);
        series.setTotalEpisodes(10);

        ContainerSummary summary = series.toSummary();

        assertThat(summary.getType(), is("series"));
        assertThat(summary.getTitle(), is(series.getTitle()));
        assertThat(summary.getDescription(), is(series.getDescription()));
        assertThat(summary.getSeriesNumber(), is(series.getSeriesNumber()));
        assertThat(summary.getTotalEpisodes(), is(series.getTotalEpisodes()));
    }

    @Test
    public void fromBrand() throws Exception {
        Brand brand = new Brand(Id.valueOf(0L), Publisher.METABROADCAST);
        brand.setTitle("title");
        brand.setDescription("description");

        ContainerSummary summary = brand.toSummary();

        assertThat(summary.getType(), is("brand"));
        assertThat(summary.getTitle(), is(brand.getTitle()));
        assertThat(summary.getDescription(), is(brand.getDescription()));
        assertThat(summary.getSeriesNumber(), nullValue());
        assertThat(summary.getTotalEpisodes(), nullValue());
    }
}
