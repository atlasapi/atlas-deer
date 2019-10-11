package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

import java.util.Objects;

@UDT(name = "contentgroupref")
public class ContentGroupRef {

    @Field(name = "id") private Long id;
    @Field(name = "uri") private String uri;

    public ContentGroupRef() {}

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        ContentGroupRef that = (ContentGroupRef) object;
        return Objects.equals(id, that.id) &&
                Objects.equals(uri, that.uri);
    }
}
