package org.atlasapi.content;

import java.util.Objects;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import static com.google.common.base.Preconditions.checkNotNull;

public class Provider {

    private String name;
    private String iconUrl;

    public Provider() {
    }

    public Provider(@NotNull String name, @Nullable String iconUrl) {
        this.name = checkNotNull(name);
        this.iconUrl = iconUrl;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Nullable
    public String getIconUrl() {
        return iconUrl;
    }

    public void setIconUrl(String iconUrl) {
        this.iconUrl = iconUrl;
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

    @Override
    public String toString() {
        return "Provider{" +
                "name='" + name + '\'' +
                ", iconUrl='" + iconUrl + '\'' +
                '}';
    }
}
