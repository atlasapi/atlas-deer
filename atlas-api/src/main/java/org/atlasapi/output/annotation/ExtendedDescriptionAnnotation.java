package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.content.Certificate;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.Film;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemSummary;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.CertificateWriter;
import org.atlasapi.output.writers.LanguageWriter;
import org.atlasapi.output.writers.PriorityReasonsWriter;
import org.atlasapi.output.writers.ReleaseDateWriter;
import org.atlasapi.output.writers.RestrictionWriter;
import org.atlasapi.output.writers.SubtitleWriter;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class ExtendedDescriptionAnnotation extends OutputAnnotation<Content, ResolvedContent> {

    private final LanguageWriter languageWriter;
    private final CertificateWriter certificateWriter;
    private final SubtitleWriter subtitleWriter;
    private final ReleaseDateWriter releaseDateWriter;
    private final RestrictionWriter restrictionWriter;
    private final PriorityReasonsWriter priorityReasonsWriter;

    public ExtendedDescriptionAnnotation() {
        super();
        this.languageWriter = new LanguageWriter(initLocalMap());
        this.certificateWriter = new CertificateWriter();
        this.subtitleWriter = new SubtitleWriter(languageWriter);
        releaseDateWriter = new ReleaseDateWriter();
        this.restrictionWriter = new RestrictionWriter();
        this.priorityReasonsWriter = new PriorityReasonsWriter();
    }

    private Map<String, Locale> initLocalMap() {
        ImmutableMap.Builder<String, Locale> builder = ImmutableMap.builder();
        for (String code : Locale.getISOLanguages()) {
            builder.put(code, new Locale(code));
        }
        return builder.build();
    }

    @Override
    public void write(ResolvedContent entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        Content desc = entity.getContent();
        writer.writeList("genres", "genre", desc.getGenres(), ctxt);
        writer.writeField("presentation_channel", desc.getPresentationChannel());
        writer.writeField(
                "priority",
                desc.getPriority() != null ? desc.getPriority().getPriority() : null
        );

        if (ctxt.getActiveAnnotations().contains(Annotation.PRIORITY_REASONS)) {
            if (desc.getPriority() != null && desc.getPriority().getReasons() != null) {
                writer.writeObject(priorityReasonsWriter, desc.getPriority().getReasons(), ctxt);
            } else {
                writer.writeObject(priorityReasonsWriter, null, ctxt);
            }
        }

        writer.writeField("short_description", desc.getShortDescription());
        writer.writeField("medium_description", desc.getMediumDescription());
        writer.writeField("long_description", desc.getLongDescription());

        if (desc instanceof Item) {
            Item item = (Item) desc;
            writer.writeField("black_and_white", item.getBlackAndWhite());
            writer.writeList("countries_of_origin", "country", item.getCountriesOfOrigin(), ctxt);
            writer.writeField("schedule_only", item.isScheduleOnly());
            writer.writeList(restrictionWriter, item.getRestrictions(), ctxt);

        }
        if (desc instanceof Container) {
            Container container = (Container) desc;
            ImmutableSet<Certificate> childCerts = container.getItemSummaries().stream()
                    .map(ItemSummary::getCertificates)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .flatMap(Collection::stream)
                    .collect(MoreCollectors.toImmutableSet());
            writer.writeList(
                    certificateWriter,
                    Sets.union(desc.getCertificates(), childCerts),
                    ctxt
            );
        } else {
            writer.writeList(certificateWriter, desc.getCertificates(), ctxt);
        }
        writer.writeList(languageWriter, desc.getLanguages(), ctxt);

        if (desc instanceof Film) {
            Film film = (Film) desc;
            writer.writeList(subtitleWriter, film.getSubtitles(), ctxt);
            writer.writeList(releaseDateWriter, film.getReleaseDates(), ctxt);
        }
    }

}
