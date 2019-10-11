package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

import java.util.Objects;

@UDT(name = "description")
public class Description {

    @Field(name = "title") private String title;
    @Field(name = "synopsis") private String synopsis;
    @Field(name = "image") private String image;
    @Field(name = "thumbnail") private String thumbnail;

    public Description() {}

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getSynopsis() {
        return synopsis;
    }

    public void setSynopsis(String synopsis) {
        this.synopsis = synopsis;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getThumbnail() {
        return thumbnail;
    }

    public void setThumbnail(String thumbnail) {
        this.thumbnail = thumbnail;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Description that = (Description) object;
        return Objects.equals(title, that.title) &&
                Objects.equals(synopsis, that.synopsis) &&
                Objects.equals(image, that.image) &&
                Objects.equals(thumbnail, that.thumbnail);
    }
}
