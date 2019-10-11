package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

import java.util.Objects;

@UDT(name = "alias")
public class Alias {

    @Field(name = "value") private String value;
    @Field(name = "namespace") private String namespace;

    public Alias() {}

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

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Alias alias = (Alias) object;
        return Objects.equals(value, alias.value) &&
                Objects.equals(namespace, alias.namespace);
    }
}
