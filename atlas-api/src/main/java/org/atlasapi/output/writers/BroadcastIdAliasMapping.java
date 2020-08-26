package org.atlasapi.output.writers;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.content.Broadcast;
import org.atlasapi.entity.Alias;
import org.atlasapi.media.entity.Publisher;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BroadcastIdAliasMapping implements Function<Broadcast, Alias> {

    private static abstract class AliasMapper {

        protected final Pattern pattern;

        public AliasMapper(String pattern) {
            this.pattern = Pattern.compile(pattern);
        }

        public Alias map(String id) {
            Matcher m = pattern.matcher(id);
            return m.matches() ? transform(id, m) : null;
        }

        public abstract Alias transform(String id, Matcher m);
    }

    private static final class GroupAliasMapper extends AliasMapper {

        private final String ns;
        private final int group;

        public GroupAliasMapper(String pattern, String ns, int group) {
            super(pattern);
            this.ns = ns;
            this.group = group;
        }

        @Override
        public final Alias transform(String id, Matcher m) {
            return new Alias(ns, m.group(group));
        }

    }

    private ImmutableSet<Publisher> enabledPublisherNamespaces = ImmutableSet.of(
            Publisher.YOUVIEW_JSON
    );

    private ImmutableList<AliasMapper> mappers = ImmutableList.<AliasMapper>builder()
            .add(new GroupAliasMapper("^gb:bt:sport:([a-zA-Z0-9\\-]*)?$", "ebs:slot", 1))
            .add(new GroupAliasMapper("^pa:([0-9]*)$", "pa:slot", 1))
            .add(new GroupAliasMapper("^bbc:([0-9a-z]*)$", "bbc:pid", 1))
            .add(new GroupAliasMapper("^bbc:([0-9a-z]*.imi:bds.tv/[0-9]*)?$", "bbc:bds", 1))
            .add(new GroupAliasMapper(
                    "^bbc:([0-9a-z]*.imi:infax.bbc.co.uk/tx_seq_num:[0-9]*:[0-9]*)?$",
                    "bbc:infax",
                    1
            ))
            .add(new GroupAliasMapper("^youview:([0-9]*)$", "youview:slot", 1))
            .add(new GroupAliasMapper("^Broadcast(C?ITV)?[0-9a-f-]*$", "itv:slot", 0))
            .add(new AliasMapper("^(c4|e4|m4|f4|4s|4m):([0-9]*)$") {

                @Override
                public Alias transform(String id, Matcher m) {
                    return new Alias("c4:" + m.group(1) + ":slot", m.group(2));
                }
            })
            .add(new GroupAliasMapper("^([0-9]*)$", "rovicorp:slot", 1))
            .addAll(
                    enabledPublisherNamespaces.stream()
                            .map(publisher ->
                                    new GroupAliasMapper(
                                            "^" + publisher.key() + ":(.*)$",
                                            publisher.key() + ":slot",
                                            1
                                    )
                            )
                            .collect(MoreCollectors.toImmutableList())
            )
            .build();

    @Override
    public
    @Nullable
    Alias apply(@Nonnull Broadcast input) {
        String id = input.getSourceId();
        if (id == null) {
            return null;
        }
        for (AliasMapper mapper : mappers) {
            Alias mapped = mapper.map(id);
            if (mapped != null) {
                return mapped;
            }
        }
        return null;
    }

}
