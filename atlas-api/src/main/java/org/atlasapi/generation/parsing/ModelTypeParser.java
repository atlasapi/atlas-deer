package org.atlasapi.generation.parsing;

import java.util.List;
import java.util.Set;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeMirror;

import org.atlasapi.generation.model.JsonType;
import org.atlasapi.generation.model.ModelMethodInfo;
import org.atlasapi.generation.model.ModelTypeInfo;
import org.atlasapi.meta.annotations.FieldName;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import static com.google.common.base.Preconditions.checkNotNull;

public class ModelTypeParser implements TypeParser<ModelTypeInfo, ModelMethodInfo> {

    private static final Set<String> NUMBER_TYPES = ImmutableSet.of(
            "int",
            "Integer",
            "long",
            "Long",
            "float",
            "Float",
            "double",
            "Double"
    );
    private final JavadocParser docParser;
    private final Set<Class<?>> classesToOutput;

    private ProcessingEnvironment processingEnv;

    public ModelTypeParser(JavadocParser docParser, Iterable<Class<?>> classesToOutput) {
        this.docParser = checkNotNull(docParser);
        this.classesToOutput = ImmutableSet.copyOf(classesToOutput);
    }

    @Override
    public void init(ProcessingEnvironment processingEnv) {
        this.processingEnv = checkNotNull(processingEnv);
    }

    @Override
    public ModelTypeInfo parse(TypeElement type) {
        return ModelTypeInfo.builder()
                .withKey(keyFrom(type))
                .withOutputClassName(classNameFrom(type))
                .withDescription(descriptionFrom(type))
                .withParsedClass(parsedClassFrom(type))
                .build();
    }

    private String keyFrom(TypeElement type) {
        return addQuotesToString(typeToSimpleName(type).toLowerCase());
    }

    private String classNameFrom(TypeElement type) {
        return typeToSimpleName(type) + "Info";
    }

    private String descriptionFrom(TypeElement type) {
        return addQuotesToString(docParser.parse(processingEnv.getElementUtils()
                .getDocComment(type)));
    }

    private String parsedClassFrom(TypeElement type) {
        return type.getQualifiedName().toString() + ".class";
    }

    private String addQuotesToString(String input) {
        return "\"" + input + "\"";
    }

    private String typeToSimpleName(TypeElement type) {
        return type.getSimpleName().toString();
    }

    @Override
    public Set<ModelMethodInfo> parse(Iterable<ExecutableElement> methods) {
        ImmutableSet.Builder<ModelMethodInfo> methodsInfo = ImmutableSet.builder();
        for (ExecutableElement method : methods) {
            methodsInfo.add(parse(method));
        }
        return methodsInfo.build();
    }

    private ModelMethodInfo parse(ExecutableElement method) {
        Optional<TypeElement> innerType = getInnerType(method.getReturnType());
        String type = typeFrom(method, innerType);
        boolean isModelClass = isModelClass(type);

        return ModelMethodInfo.builder()
                .withName(nameFrom(method))
                .withDescription(descriptionFrom(method))
                .withType(type)
                .withJsonType(jsonTypeFrom(type, innerType.isPresent(), isModelClass))
                .withIsModelType(isModelClass)
                .withIsMultiple(innerType.isPresent())
                .build();
    }

    // TODO fragile, compares simple names rather than canonical
    private boolean isModelClass(String type) {
        return Iterables.contains(Iterables.transform(
                classesToOutput,
                new Function<Class<?>, String>() {

                    @Override
                    public String apply(Class<?> input) {
                        return input.getSimpleName();
                    }
                }
        ), type);
    }

    private String nameFrom(ExecutableElement method) {
        FieldName annotation = method.getAnnotation(FieldName.class);
        return annotation.value();
    }

    private String descriptionFrom(ExecutableElement method) {
        return docParser.parse(processingEnv.getElementUtils().getDocComment(method));
    }

    private String typeFrom(ExecutableElement method, Optional<TypeElement> innerType) {
        if (innerType.isPresent()) {
            return typeToSimpleName(innerType.get());
        }

        TypeMirror returnType = method.getReturnType();
        // catch primitive types, as these break in later code
        if (returnType.getKind().isPrimitive()) {
            PrimitiveType primitive = (PrimitiveType) returnType;
            return primitive.toString();
        }
        // TODO somewhat suspect cast here...
        return typeToSimpleName((TypeElement) processingEnv.getTypeUtils().asElement(returnType));
    }

    private Optional<TypeElement> getInnerType(TypeMirror returnType) {
        if (returnType instanceof DeclaredType) {
            DeclaredType declaredType = (DeclaredType) returnType;

            List<? extends TypeMirror> typeArguments = declaredType.getTypeArguments();
            if (typeArguments.isEmpty()) {
                return Optional.absent();
            }
            // TODO this probably won't handle maps currently
            TypeMirror innerType = Iterables.getOnlyElement(typeArguments);
            return Optional.of((TypeElement) processingEnv.getTypeUtils().asElement(innerType));
        }
        return Optional.absent();
    }

    private JsonType jsonTypeFrom(String type, boolean isMultiple, boolean isModelType) {
        if (isMultiple) {
            return JsonType.ARRAY;
        }
        if (isModelType) {
            return JsonType.OBJECT;
        }
        if ("boolean".equalsIgnoreCase(type)) {
            return JsonType.BOOLEAN;
        }
        if (NUMBER_TYPES.contains(type)) {
            return JsonType.NUMBER;
        }
        // bit of a hack... assumes all types will be String unless otherwise specified
        return JsonType.STRING;
    }
}
