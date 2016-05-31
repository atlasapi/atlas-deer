package org.atlasapi.output.writers.common;

import org.atlasapi.content.Brand;
import org.atlasapi.content.ContainerSummary;
import org.atlasapi.content.Series;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.FieldWriter;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class CommonContainerSummaryWriterTest {

    @Mock private FieldWriter fieldWriter;

    private CommonContainerSummaryWriter summaryWriter;

    @Before
    public void setUp() throws Exception {
        summaryWriter = CommonContainerSummaryWriter.create();
    }

    @Test
    public void writeSeriesSummary() throws Exception {
        Series series = new Series(Id.valueOf(10L), Publisher.METABROADCAST);
        series.setTitle("title");
        series.setDescription("description");
        series.withSeriesNumber(1);
        series.setTotalEpisodes(10);

        ContainerSummary summary = series.toSummary();

        summaryWriter.write(summary, fieldWriter);

        verify(fieldWriter).writeField("type", summary.getType());
        verify(fieldWriter).writeField("title", summary.getTitle());
        verify(fieldWriter).writeField("description", summary.getDescription());
        verify(fieldWriter).writeField("series_number", summary.getSeriesNumber());
        verify(fieldWriter).writeField("total_episodes", summary.getTotalEpisodes());
    }

    @Test
    public void writeBrandSummary() throws Exception {
        Brand series = new Brand(Id.valueOf(10L), Publisher.METABROADCAST);
        series.setTitle("title");
        series.setDescription("description");

        ContainerSummary summary = series.toSummary();

        summaryWriter.write(summary, fieldWriter);

        verify(fieldWriter).writeField("type", summary.getType());
        verify(fieldWriter).writeField("title", summary.getTitle());
        verify(fieldWriter).writeField("description", summary.getDescription());
        verify(fieldWriter, never()).writeField("series_number", summary.getSeriesNumber());
        verify(fieldWriter, never()).writeField("total_episodes", summary.getTotalEpisodes());
    }
}
