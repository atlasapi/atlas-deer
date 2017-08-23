package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.Content;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.KeyPhraseWriter;

public class KeyPhrasesAnnotation extends OutputAnnotation<Content, ResolvedContent> {

    private static KeyPhraseWriter keyPhraseWriter = new KeyPhraseWriter();

    @Override
    public void write(ResolvedContent entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeList(keyPhraseWriter, entity.getContent().getKeyPhrases(), ctxt);
    }

}
