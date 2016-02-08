package org.atlasapi.generation.parsing;

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;

import org.atlasapi.generation.model.EndpointMethodInfo;
import org.atlasapi.generation.model.EndpointTypeInfo;
import org.atlasapi.meta.annotations.ProducesType;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import scala.actors.threadpool.Arrays;

import static com.google.common.base.Preconditions.checkNotNull;

// TODO this class is still pretty chunky
public class EndpointTypeParser implements TypeParser<EndpointTypeInfo, EndpointMethodInfo> {

    private final JavadocParser docParser;

    private ProcessingEnvironment processingEnv;

    public EndpointTypeParser(JavadocParser docParser) {
        this.docParser = checkNotNull(docParser);
    }

    @Override
    public void init(ProcessingEnvironment processingEnv) {
        this.processingEnv = checkNotNull(processingEnv);
    }

    @Override
    public EndpointTypeInfo parse(TypeElement type) {
        String rootPath = rootPathFrom(type);
        return EndpointTypeInfo.builder()
                .withKey(keyFrom(rootPath))
                .withClassName(classNameFrom(type))
                .withDescription(descriptionFrom(type))
                .withRootPath(rootPath)
                .withProducedType(producedTypeFrom(type))
                .build();
    }

    private String keyFrom(String rootPath) {
        return rootPath.replaceAll("(/4/)", "");
        //        return addQuotesToString(typeToSimpleName(type));
    }

    private String classNameFrom(TypeElement type) {
        return typeToSimpleName(type) + "EndpointInfo";
    }

    private String descriptionFrom(TypeElement type) {
        return addQuotesToString(docParser.parse(processingEnv.getElementUtils()
                .getDocComment(type)));
    }

    private String rootPathFrom(TypeElement type) {
        return addQuotesToString(extractRootPath(type).or(""));
    }

    private Optional<String> extractRootPath(TypeElement type) {
        RequestMapping rootMapping = type.getAnnotation(RequestMapping.class);
        if (rootMapping == null) {
            return Optional.absent();
        }
        return Optional.of(rootMapping.value()[0]);
    }

    private String producedTypeFrom(TypeElement type) {
        TypeMirror producedType = extractProducedType(type);
        // bit of a hack - we know this will be a type so can force cast it.
        // TODO check this more?
        TypeElement typeElem = (TypeElement) processingEnv.getTypeUtils().asElement(producedType);
        return addQuotesToString(typeToSimpleName(typeElem));
    }

    private TypeMirror extractProducedType(TypeElement endpointType) {
        AnnotationMirror am = getAnnotationMirror(endpointType, ProducesType.class);
        if (am == null) {
            return null;
        }
        AnnotationValue av = getAnnotationValue(am, "type");
        if (av == null) {
            return null;
        } else {
            return (TypeMirror) av.getValue();
        }
    }

    private static AnnotationMirror getAnnotationMirror(TypeElement typeElement, Class<?> clazz) {
        String clazzName = clazz.getName();
        for (AnnotationMirror m : typeElement.getAnnotationMirrors()) {
            if (m.getAnnotationType().toString().equals(clazzName)) {
                return m;
            }
        }
        return null;
    }

    private static AnnotationValue getAnnotationValue(AnnotationMirror annotationMirror,
            String key) {
        for (Entry<? extends ExecutableElement, ? extends AnnotationValue> entry : annotationMirror.getElementValues()
                .entrySet()) {
            if (entry.getKey().getSimpleName().toString().equals(key)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private String typeToSimpleName(TypeElement type) {
        return type.getSimpleName().toString();
    }

    private static String addQuotesToString(String input) {
        return "\"" + input + "\"";
    }

    // TODO this is ridiculous, with all of the nested functions. Refactor?
    @Override
    public Set<EndpointMethodInfo> parse(Iterable<ExecutableElement> methods) {
        return FluentIterable.from(methods)
                .transformAndConcat(extractMethodInfo())
                .toSet();
    }

    private Function<ExecutableElement, Iterable<EndpointMethodInfo>> extractMethodInfo() {
        return new Function<ExecutableElement, Iterable<EndpointMethodInfo>>() {

            @Override
            public Iterable<EndpointMethodInfo> apply(ExecutableElement input) {
                RequestMapping requestMappingAnnotation = input.getAnnotation(RequestMapping.class);

                Set<RequestMethod> methods = extractMethods(requestMappingAnnotation);
                Set<String> paths = extractPaths(requestMappingAnnotation);

                return FluentIterable.from(methods)
                        .transformAndConcat(mapMethodToPaths(paths));
            }
        };
    }

    private Function<RequestMethod, Iterable<EndpointMethodInfo>> mapMethodToPaths(
            final Set<String> paths) {
        return new Function<RequestMethod, Iterable<EndpointMethodInfo>>() {

            @Override
            public Iterable<EndpointMethodInfo> apply(RequestMethod input) {
                return FluentIterable.from(paths)
                        .transform(createMethodInfo(input));
            }
        };
    }

    private Function<String, EndpointMethodInfo> createMethodInfo(final RequestMethod method) {
        return new Function<String, EndpointMethodInfo>() {

            @Override
            public EndpointMethodInfo apply(String input) {
                return new EndpointMethodInfo(input, method);
            }
        };
    }

    private Set<RequestMethod> extractMethods(RequestMapping requestMapping) {
        RequestMethod[] requestMethods = requestMapping.method();
        if (requestMethods.length == 0) {
            return ImmutableSet.of(RequestMethod.GET);
        }
        return ImmutableSet.copyOf(requestMethods);
    }

    // TODO is there a way to avoid the unchecked conversion?
    private Set<String> extractPaths(RequestMapping requestMapping) {
        @SuppressWarnings("unchecked")
        List<String> paths = Arrays.asList(requestMapping.value());
        return FluentIterable.from(paths)
                .transform(quoteString())
                .toSet();
    }

    private static Function<String, String> quoteString() {
        return new Function<String, String>() {

            @Override
            public String apply(String input) {
                return addQuotesToString(input);
            }
        };
    }

}
