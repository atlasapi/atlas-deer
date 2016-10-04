package org.atlasapi.output;

import java.util.List;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.content.Content;
import org.atlasapi.output.annotation.ContainerSummaryAnnotation;
import org.atlasapi.output.annotation.ContentDescriptionAnnotation;
import org.atlasapi.output.annotation.ExtendedDescriptionAnnotation;
import org.atlasapi.output.annotation.ExtendedIdentificationAnnotation;
import org.atlasapi.output.annotation.IdentificationAnnotation;
import org.atlasapi.output.annotation.IdentificationSummaryAnnotation;
import org.atlasapi.output.annotation.OutputAnnotation;
import org.atlasapi.output.annotation.SeriesReferenceAnnotation;
import org.atlasapi.output.writers.ContainerSummaryWriter;
import org.atlasapi.output.writers.IdSummaryWriter;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.atlasapi.annotation.Annotation.DESCRIPTION;
import static org.atlasapi.annotation.Annotation.EXTENDED_DESCRIPTION;
import static org.atlasapi.annotation.Annotation.EXTENDED_ID;
import static org.atlasapi.annotation.Annotation.ID;
import static org.atlasapi.annotation.Annotation.ID_SUMMARY;
import static org.atlasapi.annotation.Annotation.SERIES_REFERENCE;
import static org.atlasapi.annotation.Annotation.SERIES_SUMMARY;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class AnnotationRegistryTest {

    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final IdentificationSummaryAnnotation idSum = IdentificationSummaryAnnotation.create(
            IdSummaryWriter.create()
    );
    private final IdentificationAnnotation ident = new IdentificationAnnotation();
    private final ExtendedIdentificationAnnotation extIdent = new ExtendedIdentificationAnnotation(
            idCodec);
    private final ContentDescriptionAnnotation desc = new ContentDescriptionAnnotation();
    private final ExtendedDescriptionAnnotation extDesc = new ExtendedDescriptionAnnotation();
    private final SeriesReferenceAnnotation seriesRef = new SeriesReferenceAnnotation(idCodec);
    private final ContainerSummaryAnnotation seriesSum = new ContainerSummaryAnnotation(
            "series",
            mock(ContainerSummaryWriter.class)
    );

    private final AnnotationRegistry<Content> registry = AnnotationRegistry.<Content>builder()
            .register(ID_SUMMARY, idSum)
            .register(ID, ident, ImmutableSet.of(ID_SUMMARY))
            .register(EXTENDED_ID, extIdent, ImmutableSet.of(ID))
            .register(SERIES_REFERENCE, seriesRef, ImmutableSet.of(ID_SUMMARY))
            .register(
                    SERIES_SUMMARY,
                    seriesSum,
                    ImmutableSet.of(ID_SUMMARY),
                    ImmutableSet.of(SERIES_REFERENCE)
            )
            .register(DESCRIPTION, desc, ImmutableSet.of(ID, SERIES_REFERENCE))
            .register(EXTENDED_DESCRIPTION, extDesc, ImmutableSet.of(DESCRIPTION, EXTENDED_ID))
            .build();

    @Test
    public void testMapsToOnlyTopLevelOutputAnnotation() {
        List<OutputAnnotation<? super Content>> annotations
                = registry.activeAnnotations(ImmutableSet.of(ID_SUMMARY));

        assertTrue(
                "mapped annotation should be id summary",
                idSum.equals(annotations.get(0))
        );
    }

    @Test
    public void testMapsImpliedAnnotations() {

        List<OutputAnnotation<? super Content>> annotations
                = registry.activeAnnotations(ImmutableSet.of(ID));

        assertTrue(
                "1st mapped annotation should be id summary",
                idSum.equals(annotations.get(0))
        );
        assertTrue(
                "2nd mapped annotation should be identification",
                ident.equals(annotations.get(1))
        );

    }

    @Test
    public void testTransitivelyImpliedAnnotations() {

        List<OutputAnnotation<? super Content>> annotations = registry.activeAnnotations(
                ImmutableSet.of(EXTENDED_DESCRIPTION));

        assertTrue("1st mapped annotation should be id summary", idSum.equals(annotations.get(0)));
        assertTrue(
                "2nd mapped annotation should be identification",
                ident.equals(annotations.get(1))
        );
        assertTrue(
                "3rd mapped annotation should be ext ident",
                extIdent.equals(annotations.get(2))
        );
        assertTrue("4th mapped annotation should be desc", desc.equals(annotations.get(3)));
        assertTrue("5th mapped annotation should be ext desc", extDesc.equals(annotations.get(4)));
        assertTrue(
                "6th mapped annotation should be series ref",
                seriesRef.equals(annotations.get(5))
        );

    }

    @Test
    public void testOverridesAnnotations() {

        List<OutputAnnotation<? super Content>> annotations = registry.activeAnnotations(
                ImmutableSet.of(EXTENDED_DESCRIPTION, SERIES_SUMMARY));

        assertTrue("1st mapped annotation should be id summary", idSum.equals(annotations.get(0)));
        assertTrue(
                "2nd mapped annotation should be identification",
                ident.equals(annotations.get(1))
        );
        assertTrue(
                "3rd mapped annotation should be ext ident",
                extIdent.equals(annotations.get(2))
        );
        assertTrue("4th mapped annotation should be desc", desc.equals(annotations.get(3)));
        assertTrue("5th mapped annotation should be ext desc", extDesc.equals(annotations.get(4)));
        assertTrue(
                "6th mapped annotation should be series summary",
                seriesSum.equals(annotations.get(5))
        );

    }

    @Test
    public void testOverridesWithMultipleImplyingAnnotations() {

        ImmutableSet<Annotation> annotationSet = ImmutableSet.of(
                EXTENDED_DESCRIPTION,
                SERIES_SUMMARY,
                DESCRIPTION
        );
        for (List<Annotation> annotationList : Collections2.permutations(annotationSet)) {
            List<OutputAnnotation<? super Content>> annotations = registry.activeAnnotations(
                    annotationList);

            assertTrue(
                    "1st mapped annotation should be id summary",
                    idSum.equals(annotations.get(0))
            );
            assertTrue(
                    "2nd mapped annotation should be identification",
                    ident.equals(annotations.get(1))
            );
            assertTrue(
                    "3rd mapped annotation should be ext ident",
                    extIdent.equals(annotations.get(2))
            );
            assertTrue("4th mapped annotation should be desc", desc.equals(annotations.get(3)));
            assertTrue(
                    "5th mapped annotation should be ext desc",
                    extDesc.equals(annotations.get(4))
            );
            assertTrue(
                    "6th mapped annotation should be series summary not " + annotations.get(5),
                    seriesSum.equals(annotations.get(5))
            );
        }

    }

}
