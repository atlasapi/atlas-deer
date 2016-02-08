package org.atlasapi.generation.output.writers;

import java.io.IOException;
import java.io.Writer;

import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.tools.Diagnostic.Kind;
import javax.tools.JavaFileObject;

import org.atlasapi.generation.model.ModelTypeInfo;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class JavaxSourceFileWriterTest {

    private Filer filer = Mockito.mock(Filer.class);
    private Messager messager = Mockito.mock(Messager.class);
    private JavaFileObject jfo = Mockito.mock(JavaFileObject.class);
    private Writer javaWriter = Mockito.mock(Writer.class);
    private final JavaxSourceFileWriter<ModelTypeInfo> writer = new JavaxSourceFileWriter<>();

    @Before
    public void setup() throws IOException {
        writer.init(filer, messager);
        Mockito.when(filer.createSourceFile(Mockito.anyString())).thenReturn(jfo);
        Mockito.when(jfo.openWriter()).thenReturn(javaWriter);
    }

    @Test
    public void testWritingOfSourceWritesFileAndLogsSource() throws IOException {
        ModelTypeInfo typeInfo = createTypeInfo();
        String source = "source";
        writer.writeFile(typeInfo, source);

        Mockito.verify(filer).createSourceFile(typeInfo.fullPackage() + "." + typeInfo.className());
        Mockito.verify(messager).printMessage(Kind.NOTE, source);

        Mockito.verify(jfo).openWriter();
        Mockito.verify(javaWriter).write(source);
        Mockito.verify(javaWriter).close();
    }

    private ModelTypeInfo createTypeInfo() {
        return ModelTypeInfo.builder()
                .withKey("key")
                .withDescription("description")
                .withOutputClassName("outputClass")
                .withParsedClass("parsedClass")
                .build();
    }

}
