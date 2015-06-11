package org.atlasapi.output.annotation;


import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import org.atlasapi.content.Content;
import org.atlasapi.content.Film;
import org.atlasapi.content.Item;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.CertificateWriter;
import org.atlasapi.output.writers.LanguageWriter;
import org.atlasapi.output.writers.ReleaseDateWriter;
import org.atlasapi.output.writers.SubtitleWriter;

import com.google.common.collect.ImmutableMap;

public class ExtendedDescriptionAnnotation extends OutputAnnotation<Content> {

    private final LanguageWriter languageWriter;
    private final CertificateWriter certificateWriter;
    private final SubtitleWriter subtitleWriter;
    private final ReleaseDateWriter releaseDateWriter;

    public ExtendedDescriptionAnnotation() {
        super();
        this.languageWriter = new LanguageWriter(initLocalMap());
        this.certificateWriter = new CertificateWriter();
        this.subtitleWriter = new SubtitleWriter(languageWriter);
        releaseDateWriter = new ReleaseDateWriter();
    }

    private Map<String, Locale> initLocalMap() {
        ImmutableMap.Builder<String, Locale> builder = ImmutableMap.builder();
        for (String code : Locale.getISOLanguages()) {
            builder.put(code, new Locale(code));
        }
        return builder.build();
    }

    @Override
    public void write(Content desc, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeList("genres", "genre", desc.getGenres(), ctxt);
        writer.writeField("presentation_channel", desc.getPresentationChannel());
        writer.writeField("priority", desc.getPriority());
        
        if (desc instanceof Item) {
            Item item = (Item) desc;
            writer.writeField("black_and_white", item.getBlackAndWhite());
            writer.writeList("countries_of_origin","country", item.getCountriesOfOrigin(), ctxt);
            writer.writeField("schedule_only", item.isScheduleOnly());
        }
        
        if (desc instanceof Content) {
            Content content = (Content) desc;
            writer.writeList(languageWriter, content.getLanguages(), ctxt);
            writer.writeList(certificateWriter, content.getCertificates(), ctxt);
        }

        if (desc instanceof Film) {
            Film film = (Film) desc;
            writer.writeList(subtitleWriter, film.getSubtitles(), ctxt);
            writer.writeList(releaseDateWriter, film.getReleaseDates(), ctxt);
        }
    }

}
