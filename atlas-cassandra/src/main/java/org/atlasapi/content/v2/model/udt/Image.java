package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.joda.time.Instant;

@UDT(name = "image")
public class Image {

    @Field(name = "u") private String uri;
    @Field(name = "t") private String type;
    @Field(name = "c") private String color;
    @Field(name = "th") private String theme;
    @Field(name = "h") private Integer height;
    @Field(name = "w") private Integer width;
    @Field(name = "ar") private String aspectRatio;
    @Field(name = "mt") private String mimeType;
    @Field(name = "as") private Instant availabilityStart;
    @Field(name = "ae") private Instant availabilityEnd;
    @Field(name = "hta") private Boolean hasTitleArt;
    @Field(name = "s") private String source;

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public String getTheme() {
        return theme;
    }

    public void setTheme(String theme) {
        this.theme = theme;
    }

    public Integer getHeight() {
        return height;
    }

    public void setHeight(Integer height) {
        this.height = height;
    }

    public Integer getWidth() {
        return width;
    }

    public void setWidth(Integer width) {
        this.width = width;
    }

    public String getAspectRatio() {
        return aspectRatio;
    }

    public void setAspectRatio(String aspectRatio) {
        this.aspectRatio = aspectRatio;
    }

    public String getMimeType() {
        return mimeType;
    }

    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }

    public Instant getAvailabilityStart() {
        return availabilityStart;
    }

    public void setAvailabilityStart(Instant availabilityStart) {
        this.availabilityStart = availabilityStart;
    }

    public Instant getAvailabilityEnd() {
        return availabilityEnd;
    }

    public void setAvailabilityEnd(Instant availabilityEnd) {
        this.availabilityEnd = availabilityEnd;
    }

    public Boolean getHasTitleArt() {
        return hasTitleArt;
    }

    public void setHasTitleArt(Boolean hasTitleArt) {
        this.hasTitleArt = hasTitleArt;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }
}
