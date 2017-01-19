package org.atlasapi.output.writers;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.application.DefaultApplication;
import org.atlasapi.content.ContainerSummary;
import org.atlasapi.content.ContainerSummaryResolver;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Series;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.common.CommonContainerSummaryWriter;

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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ContainerSummaryWriterTest {

    @Mock private NumberToShortStringCodec idCodec;
    @Mock private ContainerSummaryResolver containerSummaryResolver;
    @Mock private CommonContainerSummaryWriter commonContainerSummaryWriter;

    @Mock private FieldWriter fieldWriter;
    @Mock private OutputContext outputContext;
    @Mock private Application application;

    private ContainerSummaryWriter containerSummaryWriter;
    private ImmutableSet<Annotation> annotations;
    private Series series;
    private Episode episode;

    @Before
    public void setUp() throws Exception {
        containerSummaryWriter = ContainerSummaryWriter.create(
                idCodec,
                "container",
                containerSummaryResolver,
                commonContainerSummaryWriter
        );

        annotations = ImmutableSet.of();

        when(outputContext.getApplication()).thenReturn(application);
        when(outputContext.getActiveAnnotations()).thenReturn(annotations);

        series = new Series(Id.valueOf(10L), Publisher.METABROADCAST);
        series.setTitle("title");
        series.setDescription("description");
        series.withSeriesNumber(1);
        series.setTotalEpisodes(10);

        episode = new Episode(Id.valueOf(0L), Publisher.METABROADCAST);
        episode.setContainer(series);
    }

    @Test
    public void writeWhenEpisodeHasContainerSummary() throws Exception {
        episode.setContainerSummary(ContainerSummary.from(series));

        containerSummaryWriter.write(episode, fieldWriter, outputContext);

        ContainerSummary expectedSummary = episode.getContainerSummary();

        verify(commonContainerSummaryWriter).write(expectedSummary, fieldWriter);
    }

    @Test
    public void writeWhenContainerSummaryCanBeResolved() throws Exception {
        ContainerSummary expectedSummary = ContainerSummary.from(series);

        when(containerSummaryResolver.resolveContainerSummary(
                series.getId(), application, annotations
        ))
                .thenReturn(Optional.of(expectedSummary));

        containerSummaryWriter.write(episode, fieldWriter, outputContext);

        verify(commonContainerSummaryWriter).write(expectedSummary, fieldWriter);
    }

    @Test
    public void doNotWriteWhenContainerSummaryCannotBeResolved() throws Exception {
        when(containerSummaryResolver.resolveContainerSummary(
                series.getId(), application, annotations
        ))
                .thenReturn(Optional.absent());

        containerSummaryWriter.write(episode, fieldWriter, outputContext);

        verify(commonContainerSummaryWriter, never())
                .write(any(ContainerSummary.class), eq(fieldWriter));
    }
}
