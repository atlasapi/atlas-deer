package org.atlasapi.content;

import org.atlasapi.content.Image.AspectRatio;
import org.atlasapi.content.Image.Color;
import org.atlasapi.content.Image.Theme;
import org.atlasapi.content.Image.Type;
import org.atlasapi.entity.DateTimeSerializer;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.serialization.protobuf.CommonProtos.Image.Builder;

import com.metabroadcast.common.media.MimeType;


public class ImageSerializer {

    public CommonProtos.Image serialize(Image image) {
        
        Builder builder = CommonProtos.Image.newBuilder();
        
        if (image.getAspectRatio() != null) {
            builder.setAspectRatio(image.getAspectRatio().toString());
        }
        if (image.getAvailabilityStart() != null) {
            builder.setAvailabilityStart(new DateTimeSerializer().serialize(image.getAvailabilityStart()));
        }
        if (image.getAvailabilityEnd() != null) {
            builder.setAvailabilityEnd(new DateTimeSerializer().serialize(image.getAvailabilityEnd()));
        }
        if (image.getCanonicalUri() != null) {
            builder.setUri(image.getCanonicalUri());
        }
        if (image.getColor() != null) {
            builder.setColor(image.getColor().toString());
        }
        if (image.getHeight() != null) {
            builder.setHeight(image.getHeight());
        }
        if (image.getWidth() != null) {
            builder.setWidth(image.getWidth());
        }
        if (image.getTheme() != null) {
            builder.setTheme(image.getTheme().toString());
        }
        if (image.hasTitleArt() != null) {
            builder.setHasTitleArt(image.hasTitleArt());
        }
        if (image.getMimeType() != null) {
            builder.setMimeType(image.getMimeType().toString());
        }
        if (image.getType() != null) {
            builder.setType(image.getType().toString());
        }
        return builder.build();
    }
    
    public Image deserialize(CommonProtos.Image msg) {
        Image.Builder builder = Image.builder(msg.getUri());
        if (msg.hasAspectRatio()) {
            builder.withAspectRatio(AspectRatio.valueOf(msg.getAspectRatio()));
        }
        if (msg.hasAvailabilityStart()) {
            builder.withAvailabilityStart(new DateTimeSerializer().deserialize(msg.getAvailabilityStart()));
        }
        if (msg.hasAvailabilityStart()) {
            builder.withAvailabilityEnd(new DateTimeSerializer().deserialize(msg.getAvailabilityEnd()));
        }
        if (msg.hasColor()) {
            builder.withColor(Color.valueOf(msg.getColor()));
        }
        if (msg.hasHeight()) {
            builder.withHeight(msg.getHeight());
        }
        if (msg.hasWidth()) {
            builder.withWidth(msg.getWidth());
        }
        if (msg.hasTheme()) {
            builder.withTheme(Theme.valueOf(msg.getTheme()));
        }
        if (msg.hasHasTitleArt()) {
            builder.withHasTitleArt(msg.getHasTitleArt());
        }
        if (msg.hasMimeType()) {
            builder.withMimeType(MimeType.fromString(msg.getMimeType()));
        }
        if (msg.hasType()) {
            builder.withType(Type.valueOf(msg.getType()));
        }
        return builder.build();
    }
}
