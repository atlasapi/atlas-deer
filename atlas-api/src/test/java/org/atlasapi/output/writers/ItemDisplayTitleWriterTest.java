package org.atlasapi.output.writers;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.ContainerSummary;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Item;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.common.QueryContext;

import com.metabroadcast.common.time.DateTimeZones;

import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ItemDisplayTitleWriterTest {

    private ItemDisplayTitleWriter titleWriter = new ItemDisplayTitleWriter();
    private @Mock FieldWriter fieldWriter;

    @Test
    public void testSpecialEpisodeHasEpisodeTitleAndNoSubtitle() throws Exception {

        Episode entity = new Episode();
        entity.setSpecial(true);
        entity.setContainerSummary(new ContainerSummary(
                "brand",
                "Brand Title",
                "Brand Desc",
                null
        ));
        entity.setTitle("Episode Title");

        titleWriter.write(
                entity,
                fieldWriter,
                OutputContext.valueOf(QueryContext.standard(mock(HttpServletRequest.class)))
        );

        verifyTitle(entity.getTitle());
        verifySubtitle(null);

    }

    @Test
    public void testEpisodeWithNoContainerHasEpisodeTitleAndNoSubtitle() throws Exception {

        Episode entity = new Episode();
        entity.setSpecial(false);
        entity.setContainerSummary(null);
        entity.setTitle("Episode Title");

        titleWriter.write(
                entity,
                fieldWriter,
                OutputContext.valueOf(QueryContext.standard(mock(HttpServletRequest.class)))
        );

        verifyTitle(entity.getTitle());
        verifySubtitle(null);

    }

    @Test
    public void testEpisodeWithContainerAndNoSeriesAndEpisodeNumberAndNoSeqOrDateTitleHasContainerTitleAndSeqTitleSubtitle()
            throws Exception {

        Episode entity = new Episode();
        entity.setSpecial(false);
        entity.setContainerSummary(new ContainerSummary(
                "brand",
                "Brand Title",
                "Brand Desc",
                null
        ));
        entity.setEpisodeNumber(42);
        entity.setTitle("Episode Title");

        titleWriter.write(
                entity,
                fieldWriter,
                OutputContext.valueOf(QueryContext.standard(mock(HttpServletRequest.class)))
        );

        verifyTitle(entity.getContainerSummary().getTitle());
        verifySubtitle(String.format(
                "Episode %s: %s",
                entity.getEpisodeNumber(),
                entity.getTitle()
        ));

    }

    @Test
    public void testEpisodeWithContainerAndNoSeriesAndNoEpisodeNumberHasContainerTitleAndEpisodeTitleSubtitle()
            throws Exception {

        Episode entity = new Episode();
        entity.setSpecial(false);
        entity.setContainerSummary(new ContainerSummary(
                "brand",
                "Brand Title",
                "Brand Desc",
                null
        ));
        entity.setEpisodeNumber(null);
        entity.setTitle("Episode Title");

        titleWriter.write(
                entity,
                fieldWriter,
                OutputContext.valueOf(QueryContext.standard(mock(HttpServletRequest.class)))
        );

        verifyTitle(entity.getContainerSummary().getTitle());
        verifySubtitle(entity.getTitle());

    }

    @Test
    public void testEpisodeWithContainerAndNoSeriesAndEpisodeNumberAndSeqTitleHasContainerTitleAndEpisodeTitleSubtitle()
            throws Exception {

        Episode entity = new Episode();
        entity.setSpecial(false);
        entity.setContainerSummary(new ContainerSummary(
                "brand",
                "Brand Title",
                "Brand Desc",
                null
        ));
        entity.setEpisodeNumber(42);
        entity.setTitle("Episode 42");

        titleWriter.write(
                entity,
                fieldWriter,
                OutputContext.valueOf(QueryContext.standard(mock(HttpServletRequest.class)))
        );

        verifyTitle(entity.getContainerSummary().getTitle());
        verifySubtitle(entity.getTitle());

    }

    @Test
    public void testEpisodeWithContainerAndNoSeriesAndEpisodeNumberAndDateTitleHasContainerTitleAndEpisodeTitleSubtitle()
            throws Exception {

        Episode entity = new Episode();
        entity.setSpecial(false);
        entity.setContainerSummary(new ContainerSummary(
                "brand",
                "Brand Title",
                "Brand Desc",
                null
        ));
        entity.setEpisodeNumber(42);
        entity.setTitle("25/12/2013");

        titleWriter.write(
                entity,
                fieldWriter,
                OutputContext.valueOf(QueryContext.standard(mock(HttpServletRequest.class)))
        );

        verifyTitle(entity.getContainerSummary().getTitle());
        verifySubtitle(entity.getTitle());

    }

    @Test
    public void testEpisodeWithContainerAndNoSeriesAndEpisodeNumberAndContainerTitleHasContainerTitleAndSeqSubtitle()
            throws Exception {

        Episode entity = new Episode();
        entity.setSpecial(false);
        entity.setContainerSummary(new ContainerSummary(
                "brand",
                "Brand Title",
                "Brand Desc",
                null
        ));
        entity.setEpisodeNumber(42);
        entity.setTitle(entity.getContainerSummary().getTitle());

        titleWriter.write(
                entity,
                fieldWriter,
                OutputContext.valueOf(QueryContext.standard(mock(HttpServletRequest.class)))
        );

        verifyTitle(entity.getContainerSummary().getTitle());
        verifySubtitle(String.format("Episode %s", entity.getEpisodeNumber()));

    }

    @Test
    public void testEpisodeWithContainerAndNoSeriesAndBroadcastHasContainerTitleAndBroadcastDateSubtitle()
            throws Exception {

        Episode entity = new Episode();
        entity.setSpecial(false);
        entity.setContainerSummary(new ContainerSummary(
                "brand",
                "Brand Title",
                "Brand Desc",
                null
        ));
        entity.setEpisodeNumber(null);
        entity.setTitle(null);
        Broadcast broadcast = new Broadcast(
                Id.valueOf(1),
                new DateTime("2013-12-25T12:00:00", DateTimeZones.UTC),
                new DateTime("2013-12-26T12:00:00", DateTimeZones.UTC)
        );
        entity.addBroadcast(broadcast);

        titleWriter.write(
                entity,
                fieldWriter,
                OutputContext.valueOf(QueryContext.standard(mock(HttpServletRequest.class)))
        );

        verifyTitle(entity.getContainerSummary().getTitle());
        verifySubtitle("25/12/2013");

    }

    @Test
    public void testEpisodeWithContainerAndNoSeriesAndNoTitleEpisodeNumberAndBroadcastHasContainerTitleAndNullSubtitle()
            throws Exception {

        Episode entity = new Episode();
        entity.setSpecial(false);
        entity.setContainerSummary(new ContainerSummary(
                "brand",
                "Brand Title",
                "Brand Desc",
                null
        ));
        entity.setEpisodeNumber(null);
        entity.setTitle(null);

        titleWriter.write(
                entity,
                fieldWriter,
                OutputContext.valueOf(QueryContext.standard(mock(HttpServletRequest.class)))
        );

        verifyTitle(entity.getContainerSummary().getTitle());
        verifySubtitle(null);

    }

    @Test
    public void testEpisodeWithContainerAndSeriesWithTitleAndTitleAndEpisodeNumberHasContainerTitleAndSeriesPrefixedSubtitle()
            throws Exception {

        Episode entity = new Episode();
        entity.setSpecial(false);
        entity.setContainerSummary(new ContainerSummary(
                "brand",
                "Brand Title",
                "Brand Desc",
                null
        ));
        entity.setSeriesRef(new SeriesRef(
                Id.valueOf(1L),
                Publisher.METABROADCAST,
                "Series Title",
                24,
                new DateTime(),
                null,
                null
        ));
        entity.setEpisodeNumber(42);
        entity.setTitle("Sausages");

        titleWriter.write(
                entity,
                fieldWriter,
                OutputContext.valueOf(QueryContext.standard(mock(HttpServletRequest.class)))
        );

        verifyTitle(entity.getContainerSummary().getTitle());
        verifySubtitle("Series Title, Episode 42: Sausages");

    }

    @Test
    public void testEpisodeWithContainerAndSeriesWithNumberAndTitleAndEpisodeNumberHasContainerTitleAndSeriesNumberPrefixedSubtitle()
            throws Exception {

        Episode entity = new Episode();
        entity.setSpecial(false);
        entity.setContainerSummary(new ContainerSummary(
                "brand",
                "Brand Title",
                "Brand Desc",
                null
        ));
        entity.setSeriesRef(new SeriesRef(
                Id.valueOf(1L),
                Publisher.METABROADCAST,
                null,
                24,
                new DateTime(),
                null,
                null
        ));
        entity.setEpisodeNumber(42);
        entity.setTitle("Sausages");

        titleWriter.write(
                entity,
                fieldWriter,
                OutputContext.valueOf(QueryContext.standard(mock(HttpServletRequest.class)))
        );

        verifyTitle(entity.getContainerSummary().getTitle());
        verifySubtitle("Series 24, Episode 42: Sausages");

    }

    @Test
    public void testEpisodeWithContainerAndSeriesWithoutNumberOrTitleAndTitleAndEpisodeNumberHasContainerTitleAndEpisodeSubtitle()
            throws Exception {

        Episode entity = new Episode();
        entity.setSpecial(false);
        entity.setContainerSummary(new ContainerSummary(
                "brand",
                "Brand Title",
                "Brand Desc",
                null
        ));
        entity.setSeriesRef(new SeriesRef(
                Id.valueOf(1L),
                Publisher.METABROADCAST,
                null,
                null,
                new DateTime(),
                null,
                null
        ));
        entity.setEpisodeNumber(42);
        entity.setTitle("Sausages");

        titleWriter.write(
                entity,
                fieldWriter,
                OutputContext.valueOf(QueryContext.standard(mock(HttpServletRequest.class)))
        );

        verifyTitle(entity.getContainerSummary().getTitle());
        verifySubtitle("Episode 42: Sausages");

    }

    @Test
    public void testEpisodeWithContainerAndSeriesWithoutTitleOrEpisodeNumberOrBroadcastsHasContainerTitleAndNullSubtitle()
            throws Exception {

        Episode entity = new Episode();
        entity.setSpecial(false);
        entity.setContainerSummary(new ContainerSummary(
                "brand",
                "Brand Title",
                "Brand Desc",
                null
        ));
        entity.setSeriesRef(new SeriesRef(
                Id.valueOf(1L),
                Publisher.METABROADCAST,
                null,
                null,
                new DateTime(),
                null,
                null
        ));
        entity.setEpisodeNumber(null);
        entity.setTitle(null);

        titleWriter.write(
                entity,
                fieldWriter,
                OutputContext.valueOf(QueryContext.standard(mock(HttpServletRequest.class)))
        );

        verifyTitle(entity.getContainerSummary().getTitle());
        verifySubtitle(null);

    }

    @Test
    public void testEpisodeWithContainerAndSeriesWithBrandTitleAndTitleAndEpisodeNumberHasContainerTitleAndEpisodeSubtitle()
            throws Exception {

        Episode entity = new Episode();
        entity.setSpecial(false);
        entity.setContainerSummary(new ContainerSummary(
                "brand",
                "Brand Title",
                "Brand Desc",
                null
        ));
        entity.setSeriesRef(new SeriesRef(
                Id.valueOf(1L),
                Publisher.METABROADCAST,
                "Brand Title",
                null,
                new DateTime(),
                null,
                null
        ));
        entity.setEpisodeNumber(42);
        entity.setTitle("Sausages");

        titleWriter.write(
                entity,
                fieldWriter,
                OutputContext.valueOf(QueryContext.standard(mock(HttpServletRequest.class)))
        );

        verifyTitle(entity.getContainerSummary().getTitle());
        verifySubtitle("Episode 42: Sausages");

    }

    @Test //This should never happen
    public void testEpisodeWithoutContainerButWithSeriesIsHandledGracefully() throws Exception {

        Episode entity = new Episode();
        entity.setSpecial(false);
        entity.setContainerSummary(null);
        entity.setSeriesRef(new SeriesRef(
                Id.valueOf(1L),
                Publisher.METABROADCAST,
                "Series Title",
                24,
                new DateTime(),
                null,
                null
        ));
        entity.setEpisodeNumber(42);
        entity.setTitle("Sausages");

        titleWriter.write(
                entity,
                fieldWriter,
                OutputContext.valueOf(QueryContext.standard(mock(HttpServletRequest.class)))
        );

        verifyTitle("Sausages");
        verifySubtitle(null);

    }

    @Test
    public void testItemWithContainerTitleAndNotTitleEpisodeNumberOrBroadcastHasContainerTitleAndNullSubtitle()
            throws Exception {

        Item entity = new Item();
        entity.setContainerSummary(new ContainerSummary(
                "brand",
                "Brand Title",
                "Brand Desc",
                null
        ));
        entity.setTitle(null);

        titleWriter.write(
                entity,
                fieldWriter,
                OutputContext.valueOf(QueryContext.standard(mock(HttpServletRequest.class)))
        );

        verifyTitle(entity.getContainerSummary().getTitle());
        verifySubtitle(null);

    }

    @Test
    public void testItemWithContainerWithoutTitleAndNoTitleEpisodeNumberOrBroadcastHasNullTitleAndNullSubtitle()
            throws Exception {

        Item entity = new Item();
        entity.setContainerSummary(new ContainerSummary("brand", null, "Brand Desc", null));
        entity.setTitle(null);

        titleWriter.write(
                entity,
                fieldWriter,
                OutputContext.valueOf(QueryContext.standard(mock(HttpServletRequest.class)))
        );

        verifyTitle(null);
        verifySubtitle(null);

    }

    @Test
    public void testItemWithoutContainerWithoutTitleEpisodeNumberOrBroadcastHasNullTitleAndNullSubtitle()
            throws Exception {

        Item entity = new Item();
        entity.setContainerSummary(null);
        entity.setTitle(null);

        titleWriter.write(
                entity,
                fieldWriter,
                OutputContext.valueOf(QueryContext.standard(mock(HttpServletRequest.class)))
        );

        verifyTitle(null);
        verifySubtitle(null);

    }

    @Test
    public void testEpisodeWithPartTitle() throws Exception {

        Episode entity = new Episode();
        entity.setSpecial(false);
        entity.setContainerSummary(new ContainerSummary("brand", "Silent Witness", "Shhh", null));
        entity.setSeriesRef(new SeriesRef(
                Id.valueOf(1L),
                Publisher.METABROADCAST,
                "Series 17",
                17,
                new DateTime(),
                null,
                null
        ));
        entity.setEpisodeNumber(5);
        entity.setTitle("Fraternity Part 2");

        titleWriter.write(
                entity,
                fieldWriter,
                OutputContext.valueOf(QueryContext.standard(mock(HttpServletRequest.class)))
        );

        verifyTitle("Silent Witness");
        verifySubtitle("Series 17, Episode 5: Fraternity Part 2");

    }

    private void verifyTitle(String title) throws IOException {
        verify(fieldWriter).writeField("title", title);
    }

    private void verifySubtitle(String subtitle) throws IOException {
        verify(fieldWriter).writeField("subtitle", subtitle);
    }
}
