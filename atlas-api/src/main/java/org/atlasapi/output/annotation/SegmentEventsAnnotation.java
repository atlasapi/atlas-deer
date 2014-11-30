package org.atlasapi.output.annotation;


import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.content.RelatedLink;
import org.atlasapi.entity.Id;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.SegmentRelatedLinkMerger;
import org.atlasapi.output.SegmentRelatedLinkMergingFetcher;
import org.atlasapi.segment.Segment;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.segment.SegmentResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;


public class SegmentEventsAnnotation extends OutputAnnotation<Content> {

    private final SegmentRelatedLinkMergingFetcher linkMergingFetcher;
    private final SegmentResolver segmentResolver;
    private final Logger log = LoggerFactory.getLogger(getClass());

    public SegmentEventsAnnotation(SegmentRelatedLinkMergingFetcher linkMergingFetcher, SegmentResolver segmentResolver) {
        this.linkMergingFetcher = checkNotNull(linkMergingFetcher);
        this.segmentResolver = checkNotNull(segmentResolver);
    }

    @Override
    public void write(Content entity, FieldWriter format, OutputContext ctxt) throws IOException {
        //TODO this;
        if (entity instanceof Item) {
            ImmutableList<RelatedLink> links = linkMergingFetcher.fetchAndMergeRelatedLinks((Item) entity);
        }
    }
}