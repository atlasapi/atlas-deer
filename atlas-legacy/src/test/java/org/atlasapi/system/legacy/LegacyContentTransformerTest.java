package org.atlasapi.system.legacy;


import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Series;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LegacyContentTransformerTest {

    @Mock
    private ChannelResolver channelResolver;

    @Mock
    private LegacySegmentMigrator legacySegmentMigrator;

    @InjectMocks
    private LegacyContentTransformer objectUnderTest;


    @Test
    public void testTransformSeriesWithParentRefWithNullId() {
        Series legacy = new Series();
        legacy.setId(1L);
        legacy.setParentRef(new ParentRef("parentUrl"));

        objectUnderTest.apply(legacy);
    }

}