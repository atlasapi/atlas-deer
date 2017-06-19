package org.atlasapi.output.writers;

import com.google.common.collect.Lists;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.content.ChannelVariantRef;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.ChannelGroupSummaryWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.v4.channel.ChannelWriter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ChannelWriterTest {

    @Mock private NumberToShortStringCodec codec;

    @Mock private FieldWriter writer;
    @Mock private OutputContext context;
    @Mock private HttpServletRequest request;

    private ChannelWriter channelWriter;
    private ResolvedChannel resolvedChannelWithoutVariants;
    private ResolvedChannel resolvedChannelWithVariants;

    private List<ChannelVariantRef> includedRefs;
    private List<ChannelVariantRef> excludedRefs;

    @Before
    public void setUp() throws Exception {

        when(request.getParameter(any(String.class))).thenReturn("");
        when(context.getRequest()).thenReturn(request);

        channelWriter = ChannelWriter.create(
                "list", "field", ChannelGroupSummaryWriter.create(codec)
        );

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1234L);

        resolvedChannelWithoutVariants = ResolvedChannel.builder(channel).build();

        includedRefs = Lists.newArrayList(ChannelVariantRef.create("included", Id.valueOf(123L)));

        excludedRefs = Lists.newArrayList(ChannelVariantRef.create("excluded", Id.valueOf(456L)));

        resolvedChannelWithVariants = ResolvedChannel.builder(channel)
                        .withIncludedVariants(includedRefs)
                        .withExcludedVariants(excludedRefs)
                        .build();

    }

    @Test
    public void DoesNotWriteIncludedAndExcludedVariantsWhenNotPresent() throws Exception {
        channelWriter.write(resolvedChannelWithoutVariants, writer, context);

        verify(writer, never()).writeList(any(ChannelVariantRefWriter.class), eq(includedRefs), eq(context));
        verify(writer, never()).writeList(any(ChannelVariantRefWriter.class), eq(excludedRefs), eq(context));

    }

    @Test
    public void writesIncludedAndExcludedVariantsWhenPresent() throws Exception {
        channelWriter.write(resolvedChannelWithVariants, writer, context);

        verify(writer).writeList(any(ChannelVariantRefWriter.class), eq(includedRefs), eq(context));
        verify(writer).writeList(any(ChannelVariantRefWriter.class), eq(excludedRefs), eq(context));

    }
}
