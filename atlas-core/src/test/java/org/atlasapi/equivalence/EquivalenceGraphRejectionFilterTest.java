package org.atlasapi.equivalence;

import java.util.stream.IntStream;

import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class EquivalenceGraphRejectionFilterTest {

    private EquivalenceGraphRejectionFilter rejectionFilter;

    private ResourceRef subject;

    @Before
    public void setUp() throws Exception {
        rejectionFilter = EquivalenceGraphRejectionFilter.create();

        subject = new ItemRef(
                Id.valueOf(0L),
                Publisher.METABROADCAST,
                "",
                DateTime.now()
        );
    }

    @Test
    public void returnOkForUpdateWithGraphsSmallerThanAlertingThreshold() throws Exception {
        EquivalenceGraphRejectionFilter.Decision decision = rejectionFilter.shouldReject(
                subject,
                ImmutableSet.of(),
                EquivalenceGraphUpdate
                        .builder(getGraphOfSize(50))
                        .build()
        );

        assertThat(decision, is(EquivalenceGraphRejectionFilter.Decision.OK));
    }

    @Test
    public void returnOkForUpdateWithGraphsSmallerThanAlertingThresholdIfExistingAreLarge()
            throws Exception {
        EquivalenceGraphRejectionFilter.Decision decision = rejectionFilter.shouldReject(
                subject,
                ImmutableSet.of(getGraphOfSize(500)),
                EquivalenceGraphUpdate
                        .builder(getGraphOfSize(50))
                        .build()
        );

        assertThat(decision, is(EquivalenceGraphRejectionFilter.Decision.OK));
    }

    @Test
    public void returnWarnForUpdateWithGraphsOverTheWarningThresholdButUnderRejectThreshold()
            throws Exception {
        EquivalenceGraphRejectionFilter.Decision decision = rejectionFilter.shouldReject(
                subject,
                ImmutableSet.of(),
                EquivalenceGraphUpdate
                        .builder(getGraphOfSize(50))
                        .withCreated(ImmutableSet.of(getGraphOfSize(200)))
                        .build()
        );

        assertThat(decision, is(EquivalenceGraphRejectionFilter.Decision.WARN));
    }

    @Test
    public void returnFailForUpdateWithGraphsOverTheRejectThresholdThatHaveGrown()
            throws Exception {
        EquivalenceGraphRejectionFilter.Decision decision = rejectionFilter.shouldReject(
                subject,
                ImmutableSet.of(getGraphOfSize(100)),
                EquivalenceGraphUpdate
                        .builder(getGraphOfSize(50))
                        .withCreated(ImmutableSet.of(getGraphOfSize(300)))
                        .build()
        );

        assertThat(decision, is(EquivalenceGraphRejectionFilter.Decision.FAIL));
    }

    @Test
    public void returnWarnForUpdateWithGraphsOverTheRejectThresholdThatHaveShrunk()
            throws Exception {
        EquivalenceGraphRejectionFilter.Decision decision = rejectionFilter.shouldReject(
                subject,
                ImmutableSet.of(getGraphOfSize(301)),
                EquivalenceGraphUpdate
                        .builder(getGraphOfSize(50))
                        .withCreated(ImmutableSet.of(getGraphOfSize(300)))
                        .build()
        );

        assertThat(decision, is(EquivalenceGraphRejectionFilter.Decision.WARN));
    }

    private EquivalenceGraph getGraphOfSize(int size) {
        ResourceRef entryPoint = new ItemRef(
                Id.valueOf(0L),
                Publisher.METABROADCAST,
                "",
                DateTime.now()
        );

        ImmutableSet<ResourceRef> refs = IntStream.range(1, size)
                .mapToObj(index -> new ItemRef(
                        Id.valueOf(index),
                        Publisher.METABROADCAST,
                        "",
                        DateTime.now()
                ))
                .collect(MoreCollectors.toImmutableSet());

        EquivalenceGraph.Adjacents entryPointAdjacent = EquivalenceGraph.Adjacents
                .valueOf(entryPoint)
                .copyWithOutgoing(ImmutableSet.<ResourceRef>builder()
                        .add(entryPoint)
                        .addAll(refs)
                        .build());

        ImmutableSet<EquivalenceGraph.Adjacents> adjacents = refs.stream()
                .map(EquivalenceGraph.Adjacents::valueOf)
                .map(adjacent -> adjacent.copyWithIncoming(entryPoint))
                .collect(MoreCollectors.toImmutableSet());

        return EquivalenceGraph.valueOf(ImmutableSet.<EquivalenceGraph.Adjacents>builder()
                .add(entryPointAdjacent)
                .addAll(adjacents)
                .build());
    }
}
