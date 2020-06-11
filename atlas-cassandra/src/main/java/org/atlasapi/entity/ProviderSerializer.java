package org.atlasapi.entity;

import org.atlasapi.content.Provider;
import org.atlasapi.serialization.protobuf.ContentProtos;

public class ProviderSerializer {

    public ContentProtos.Provider serialize(Provider provider) {
        ContentProtos.Provider.Builder providerBuilder = ContentProtos.Provider.newBuilder();

        providerBuilder.setName(provider.getName());
        if (provider.getIconUrl() != null) {
            providerBuilder.setIconUrl(provider.getIconUrl());
        }

        return providerBuilder.build();
    }

    public Provider deserialize(ContentProtos.Provider providerBuffer) {
        if (providerBuffer.hasName()) {
            return new Provider(
                    providerBuffer.getName(),
                    providerBuffer.hasIconUrl() ? providerBuffer.getIconUrl() : null
            );
        }

        return null;
    }
}
