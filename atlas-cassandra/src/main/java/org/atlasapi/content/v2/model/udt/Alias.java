package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.UDT;

@UDT(name = "alias")
public class Alias {

    private String value;
    private String namespace;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
}
