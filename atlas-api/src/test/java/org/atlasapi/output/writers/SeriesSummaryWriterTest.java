package org.atlasapi.output.writers;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.content.ContainerSummary;
import org.atlasapi.content.ContainerSummaryResolver;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Series;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.common.CommonContainerSummaryWriter;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.ids.NumberToShortStringCodec;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SeriesSummaryWriterTest {

    @Mock private NumberToShortStringCodec idCodec;
    @Mock private ContainerSummaryResolver containerSummaryResolver;
    @Mock private CommonContainerSummaryWriter commonContainerSummaryWriter;

    @Mock private FieldWriter fieldWriter;
    @Mock private OutputContext outputContext;
    @Mock private Application application;

    private SeriesSummaryWriter seriesSummaryWriter;

    private ImmutableSet<Annotation> annotations;
    private Series series;
    private Episode episode;

    @Before
    public void setUp() throws Exception {
        seriesSummaryWriter = SeriesSummaryWriter.create(
                idCodec,
                containerSummaryResolver,
                commonContainerSummaryWriter
        );

        annotations = ImmutableSet.of();

        when(outputContext.getApplication()).thenReturn(application);
        when(outputContext.getActiveAnnotations()).thenReturn(annotations);
        when(outputContext.getOperands()).thenReturn(ImmutableSet.of());

        series = new Series(Id.valueOf(10L), Publisher.METABROADCAST);

        episode = new Episode(Id.valueOf(0L), Publisher.METABROADCAST);
        episode.setSeries(series);
    }

    @Test
    public void writeWhenSummaryCanBeResolved() throws Exception {
        ContainerSummary expectedSummary = ContainerSummary.from(series);

        when(containerSummaryResolver.resolveContainerSummary(
                series.getId(),
                application,
                annotations,
                ImmutableSet.of()
        )).thenReturn(Optional.of(expectedSummary));

        seriesSummaryWriter.write(episode, fieldWriter, outputContext);

        verify(commonContainerSummaryWriter).write(expectedSummary, fieldWriter);
    }

    @Test
    public void doNotWriteWhenSummaryCannotBeResolved() throws Exception {
        when(containerSummaryResolver.resolveContainerSummary(
                series.getId(),
                application,
                annotations,
                ImmutableSet.of()
        )).thenReturn(Optional.absent());

        seriesSummaryWriter.write(episode, fieldWriter, outputContext);

        verify(commonContainerSummaryWriter, never())
                .write(any(ContainerSummary.class), eq(fieldWriter));
    }
}
