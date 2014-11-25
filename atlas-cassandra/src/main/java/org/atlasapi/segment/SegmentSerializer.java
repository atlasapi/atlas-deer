package org.atlasapi.segment;

import static org.atlasapi.serialization.protobuf.SegmentProtos.Segment.Builder;

import java.nio.ByteBuffer;

import javax.annotation.Nullable;

import org.atlasapi.content.RelatedLinkSerializer;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.ProtoBufUtils;
import org.atlasapi.entity.Serializer;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.serialization.protobuf.SegmentProtos;
import org.joda.time.Duration;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.protobuf.InvalidProtocolBufferException;


public class SegmentSerializer implements Serializer<Segment, byte[]> {

    public static final Function<Alias, CommonProtos.Alias> ALIAS_TO_PROTO =
            new Function<Alias, CommonProtos.Alias>() {

        @Override
        public CommonProtos.Alias apply(@Nullable Alias alias) {
            return CommonProtos.Alias.newBuilder().setValue(alias.getValue()).setNamespace(alias.getNamespace()).build();
        }
    };

    public static final Function<CommonProtos.Alias, Alias> PROTO_TO_ALIAS =
            new Function<CommonProtos.Alias, Alias>() {
                @Nullable
                @Override
                public Alias apply(@Nullable CommonProtos.Alias alias) {
                    return new Alias(alias.getNamespace(), alias.getValue());
                }
            };


    private final RelatedLinkSerializer relatedLinkSerializer = new RelatedLinkSerializer();

    @Override
    public byte[] serialize(Segment src) {
        Builder builder = SegmentProtos.Segment.newBuilder();
        builder.setId(src.getId().longValue());
        builder.setSource(src.getPublisher().key());
        if (src.getDuration() != null) {
            builder.setDuration(src.getDuration().toStandardSeconds().getSeconds());
        }
        if (src.getTitle() != null) {
            builder.setTitle(src.getTitle());
        }
        if (src.getAliases() != null && !src.getAliases().isEmpty()) {
            builder.addAllAliases(Iterables.transform(src.getAliases(), ALIAS_TO_PROTO));
        }
        if (src.getType() != null) {
            builder.setType(src.getType().toString());
        }
        if (src.getRelatedLinks() != null && !src.getRelatedLinks().isEmpty()) {
            builder.addAllLinks(Iterables.transform(src.getRelatedLinks(), relatedLinkSerializer.TO_PROTO));
        }
        if (src.getShortDescription() != null) {
            builder.setShortDescription(src.getShortDescription());
        }
        if (src.getMediumDescription() != null) {
            builder.setMediumDescription(src.getMediumDescription());
        }
        if (src.getLongDescription() != null) {
            builder.setLongDescription(src.getLongDescription());
        }
        if (src.getFirstSeen() != null) {
            builder.setFirstSeen(ProtoBufUtils.serializeDateTime(src.getFirstSeen()));
        }
        if (src.getLastFetched() != null) {
            builder.setLastFetched(ProtoBufUtils.serializeDateTime(src.getLastFetched()));
        }
        if (src.getThisOrChildLastUpdated() != null) {
            builder.setThisOrChildLastUpdated(ProtoBufUtils.serializeDateTime(src.getThisOrChildLastUpdated()));
        }
        if (src.getLastUpdated() != null) {
            builder.setLastUpdated(ProtoBufUtils.serializeDateTime(src.getLastUpdated()));
        }
        return builder.build().toByteArray();
    }

    @Override
    public Segment deserialize(byte[] msg) {
        try {
            SegmentProtos.Segment proto = SegmentProtos.Segment.parseFrom(msg);
            Segment segment = new Segment();
            segment.setId(proto.getId());
            segment.setPublisher(Publisher.fromKey(proto.getSource()).requireValue());
            segment.setDuration(Duration.standardSeconds(proto.getDuration()));
            segment.setTitle(proto.getTitle());
            segment.setAliases(Iterables.transform(proto.getAliasesList(), PROTO_TO_ALIAS));
            segment.setType(SegmentType.fromString(proto.getType()).requireValue());
            segment.setShortDescription(proto.getShortDescription());
            segment.setMediumDescription(proto.getMediumDescription());
            segment.setLongDescription(proto.getLongDescription());
            segment.setFirstSeen(ProtoBufUtils.deserializeDateTime(proto.getFirstSeen()));
            segment.setLastUpdated(ProtoBufUtils.deserializeDateTime(proto.getLastUpdated()));
            segment.setLastFetched(ProtoBufUtils.deserializeDateTime(proto.getLastFetched()));
            segment.setThisOrChildLastUpdated(ProtoBufUtils.deserializeDateTime(proto.getThisOrChildLastUpdated()));
            segment.setRelatedLinks(Iterables.transform(proto.getLinksList(), relatedLinkSerializer.FROM_PROTO));
            return segment;
        } catch (InvalidProtocolBufferException e) {
            throw Throwables.propagate(e);
        }
    }
}
