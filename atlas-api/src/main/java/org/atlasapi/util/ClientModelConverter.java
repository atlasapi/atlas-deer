package org.atlasapi.util;

import org.atlasapi.content.Content;
import org.atlasapi.content.Item;

public class ClientModelConverter {

    public Content convert(org.atlasapi.deer.client.model.types.Content api) {
        return new Item();
    }
}
