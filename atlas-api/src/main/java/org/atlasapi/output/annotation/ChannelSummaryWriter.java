package org.atlasapi.output.annotation;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.content.Image;
import org.atlasapi.entity.Alias;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.AliasWriter;
import org.atlasapi.output.writers.ImageListWriter;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

public class ChannelSummaryWriter extends OutputAnnotation<Channel, Channel> {

    private final NumberToShortStringCodec codec;

    private final EntityListWriter<Alias> aliasWriter = new AliasWriter();
    private final EntityListWriter<Image> imageWriter = new ImageListWriter();

    private static final Function<org.atlasapi.media.entity.Alias, Alias> toV4Alias =
            input -> new Alias(input.getNamespace(), input.getValue());

    public ChannelSummaryWriter(NumberToShortStringCodec codec) {
        super();
        this.codec = codec;
    }

    @Override
    public void write(Channel entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("id", codec.encode(BigInteger.valueOf(entity.getId())));
        writer.writeField("type", entity.getClass().getSimpleName().toLowerCase());
        writer.writeList(aliasWriter, Iterables.transform(entity.getAliases(), toV4Alias), ctxt);
        writer.writeField("title", entity.getTitle());
        writer.writeList(imageWriter, transform(entity.getImages()), ctxt);
    }

    private Iterable<Image> transform(Set<org.atlasapi.media.entity.Image> images) {
        return images.stream().map(input -> {
            Image image = new Image(input.getCanonicalUri());
            image.setType(transformEnum(input.getType(), Image.Type.class));
            image.setColor(transformEnum(input.getColor(), Image.Color.class));
            image.setTheme(transformEnum(input.getTheme(), Image.Theme.class));
            image.setHeight(input.getHeight());
            image.setWidth(input.getWidth());
            image.setAspectRatio(transformEnum(
                    input.getAspectRatio(),
                    Image.AspectRatio.class
            ));
            image.setMimeType(input.getMimeType());
            image.setAvailabilityStart(input.getAvailabilityStart());
            image.setAvailabilityEnd(input.getAvailabilityEnd());
            return image;
        }).collect(Collectors.toList());
    }

    protected <E extends Enum<E>> E transformEnum(Enum<?> from, Class<E> to) {
        if (from == null) {
            return null;
        }
        try {
            return Enum.valueOf(to, from.name());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

}
