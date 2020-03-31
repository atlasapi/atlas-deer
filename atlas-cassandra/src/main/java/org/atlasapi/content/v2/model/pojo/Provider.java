package org.atlasapi.content.v2.model.pojo;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class Provider {

    private String name;
    private String iconUrl;

    @Nullable
    public String getIconUrl() {
        return iconUrl;
    }

    public void setIconUrl(String iconUrl) {
        this.iconUrl = iconUrl;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Provider() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Provider provider = (Provider) o;
        return name.equals(provider.name) &&
                Objects.equals(iconUrl, provider.iconUrl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, iconUrl);
    }
}
