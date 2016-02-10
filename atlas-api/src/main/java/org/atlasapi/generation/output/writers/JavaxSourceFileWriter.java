package org.atlasapi.generation.output.writers;

import java.io.IOException;
import java.io.Writer;

import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.tools.Diagnostic.Kind;
import javax.tools.JavaFileObject;

import org.atlasapi.generation.model.TypeInfo;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;

import static com.google.common.base.Preconditions.checkNotNull;

public final class JavaxSourceFileWriter<TI extends TypeInfo> implements SourceFileWriter<TI> {

    private Filer filer;
    private Messager messager;

    public JavaxSourceFileWriter() {
    }

    @Override
    public void init(Filer filer, Messager messager) {
        this.filer = checkNotNull(filer);
        this.messager = checkNotNull(messager);
    }

    @Override
    public void writeFile(TI typeInfo, String typeSource) {
        messager.printMessage(Kind.NOTE, typeSource);
        try {
            String fullName = Joiner.on('.').join(typeInfo.fullPackage(), typeInfo.className());
            JavaFileObject srcFile = filer.createSourceFile(fullName);
            Writer srcWriter = srcFile.openWriter();
            try {
                srcWriter.write(typeSource);
            } finally {
                srcWriter.close();
            }
        } catch (IOException e) {
            messager.printMessage(
                    Kind.ERROR,
                    "Failed to write "
                            + typeInfo.className()
                            + ": "
                            + Throwables.getStackTraceAsString(e)
            );
        }
    }
}
