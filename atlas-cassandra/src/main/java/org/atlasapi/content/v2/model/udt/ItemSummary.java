package org.atlasapi.content.v2.model.udt;

import java.util.Set;

import com.datastax.driver.mapping.annotations.UDT;

@UDT(name = "itemsummary")
public class ItemSummary {

    private ItemRef ref;
    private String title;
    private String description;
    private String image;
    private Integer releaseYear;
    private Set<Certificate> certificate;

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

    public void setCertificate(
            Set<Certificate> certificate) {
        this.certificate = certificate;
    }
}
