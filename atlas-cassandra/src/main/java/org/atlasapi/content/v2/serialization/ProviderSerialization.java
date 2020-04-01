package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.pojo.Provider;

public class ProviderSerialization {

    public Provider serialize(org.atlasapi.content.Provider provider) {
        if (provider == null || provider.getName() == null) {
            return null;
        }
        Provider internal = new Provider();
        internal.setName(provider.getName());
        if (provider.getIconUrl() != null) {
            internal.setIconUrl(provider.getIconUrl());
        }

        return internal;
    }

    public org.atlasapi.content.Provider deserialize(Provider internal) {
        if (internal == null) {
            return null;
        }
        org.atlasapi.content.Provider provider = new org.atlasapi.content.Provider();

        provider.setName(internal.getName());
        if (internal.getIconUrl() != null) {
            provider.setIconUrl(internal.getIconUrl());
        }

        return provider;
    }
}
