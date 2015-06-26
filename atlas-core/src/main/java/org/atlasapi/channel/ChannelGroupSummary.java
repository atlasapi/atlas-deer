package org.atlasapi.channel;

import com.google.common.collect.ImmutableSet;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupSummary {

    private final Id id;
    private final Set<Alias> aliases;
    private final String title;
    private final String type;


    public ChannelGroupSummary(Id id, Set<Alias> aliases, String title, String type) {
        this.id = checkNotNull(id);
        this.aliases = ImmutableSet.copyOf(aliases);
        this.title = checkNotNull(title);
        this.type = checkNotNull(type);
    }

    public Id getId() {
        return id;
    }

    public Set<Alias> getAliases() {
        return aliases;
    }

    public String getTitle() {
        return title;
    }

    public String getType() {
        return type;
    }
}
