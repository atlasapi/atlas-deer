package org.atlasapi.content.v2.model.udt;

import java.util.Set;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

@UDT(name = "itemsummary")
public class ItemSummary {

    @Field(name = "r")
    private ItemRef ref;

    @Field(name = "t")
    private String title;

    @Field(name = "d")
    private String description;

    @Field(name = "i")
    private String image;

    @Field(name = "ry")
    private Integer releaseYear;

    @Field(name = "c")
    private Set<Certificate> certificate;

    @Field(name = "tp")
    private String type;

    @Field(name = "en")
    private Integer episodeNumber;

    public ItemRef getRef() {
        return ref;
    }

    public void setRef(ItemRef ref) {
        this.ref = ref;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public Integer getReleaseYear() {
        return releaseYear;
    }

    public void setReleaseYear(Integer releaseYear) {
        this.releaseYear = releaseYear;
    }

    public Set<Certificate> getCertificate() {
        return certificate;
    }

    public void setCertificate(Set<Certificate> certificate) {
        this.certificate = certificate;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Integer getEpisodeNumber() {
        return episodeNumber;
    }

    public void setEpisodeNumber(Integer episodeNumber) {
        this.episodeNumber = episodeNumber;
    }
}
