package org.atlasapi.output.writers;

import org.atlasapi.content.Player;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import javax.annotation.Nonnull;
import java.io.IOException;

public class PlayerWriter implements EntityWriter<Player> {
    @Override
    public void write(@Nonnull Player entity, @Nonnull FieldWriter writer, @Nonnull OutputContext ctxt) throws IOException {

    }

    @Nonnull
    @Override
    public String fieldName(Player entity) {
        return "player";
    }
}
