package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.UDT;

@UDT(name = "ref")
public class Ref {

    private Long id;
    private String source;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }
}
