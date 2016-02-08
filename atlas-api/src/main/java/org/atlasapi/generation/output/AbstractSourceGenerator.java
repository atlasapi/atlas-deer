package org.atlasapi.generation.output;

import java.util.Set;

import org.atlasapi.generation.model.MethodInfo;
import org.atlasapi.generation.model.TypeInfo;

public abstract class AbstractSourceGenerator<TI extends TypeInfo, MI extends MethodInfo>
        implements SourceGenerator<TI, MI> {

    private static final String PACKAGE_DECLARATION = "package %s;\n";
    private static final String IMPORT_DECLARATION = "import %s;\n";
    private static final String CLASS_DECLARATION = "public class %s implements %s {\n";

    protected static final String NEWLINE = "\n";
    protected static final String TAB = "    ";

    @Override
    public final String processType(TI typeInfo, Iterable<MI> methodsInfo) {
        return generateClass(typeInfo, methodsInfo);
    }

    private String generateClass(TI typeInfo, Iterable<MI> methodsInfo) {
        StringBuilder source = new StringBuilder();

        source.append(generateClassHeader(typeInfo));
        source.append(NEWLINE);
        source.append(generateMethodBasedFields(methodsInfo));
        source.append(NEWLINE);
        source.append(generateTypeBasedFields(typeInfo));
        source.append(NEWLINE);
        source.append(generateClassFooter());

        return source.toString();
    }

    private String generateClassHeader(TI typeInfo) {
        StringBuilder header = new StringBuilder();

        header.append(String.format(PACKAGE_DECLARATION, typeInfo.fullPackage()));
        header.append(NEWLINE);
        header.append(generateImports());
        header.append(NEWLINE);
        header.append(NEWLINE);
        header.append(generateClassDeclaration(typeInfo));

        return header.toString();
    }

    private String generateImports() {
        StringBuilder imports = new StringBuilder();
        for (Class<?> importedClass : importedClasses()) {
            imports.append(String.format(
                    IMPORT_DECLARATION,
                    importedClass.getCanonicalName().toString()
            ));
        }
        return imports.toString();
    }

    private String generateClassDeclaration(TI typeInfo) {
        return String.format(
                CLASS_DECLARATION,
                typeInfo.className(),
                inheritedInterfaceName().getSimpleName().toString()
        );
    }

    // makes set of certain type, containing an element for each method, and a method to access that set
    private String generateMethodBasedFields(Iterable<MI> methodsInfo) {
        StringBuilder methodBasedFields = new StringBuilder();

        methodBasedFields.append(createSetFromMethods(setName(), setType(), methodsInfo));
        methodBasedFields.append(NEWLINE);
        methodBasedFields.append(formatOverriddenMethod(
                setName(),
                String.format("Set<%s>", setType().getSimpleName().toString()),
                setName()
        ));

        return methodBasedFields.toString();
    }

    private String createSetFromMethods(String fieldInfoSetName, Class<?> setType,
            Iterable<MI> methodsInfo) {
        StringBuilder setFromMethods = new StringBuilder();

        setFromMethods.append(String.format(
                "%3$sprivate static final Set<%2$s> %1$s = ImmutableSet.<%2$s>builder()\n",
                fieldInfoSetName,
                setType.getSimpleName().toString(),
                TAB
        ));

        for (MI method : methodsInfo) {
            // TODO this is vomit-worthy
            // get rid of these darn tabs
            setFromMethods.append(String.format(
                    "%2$s.add(\n%1$s\n%2$s)\n",
                    setElementFromMethod(method, TAB + TAB + TAB + TAB),
                    TAB + TAB + TAB
            ));
        }

        setFromMethods.append(String.format("%1$s%1$s%1$s.build();\n", TAB));

        return setFromMethods.toString();
    }

    private String generateClassFooter() {
        return "}\n";
    }

    // TODO can the triple String method signature be changed?
    // return type has to be a string, as otherwise can't represent collections and their internal types
    protected String formatOverriddenMethod(String methodName, String returnType,
            String returnValue) {
        StringBuilder method = new StringBuilder()
                .append(TAB)
                .append("@Override\n")

                .append(TAB)
                .append(String.format("public %s %s() {\n", returnType, methodName))

                .append(TAB)
                .append(TAB)
                .append(String.format("return %s;\n", returnValue))

                .append(TAB)
                .append("}\n");

        return method.toString();
    }

    // TODO should this live here?
    protected String addQuotesToString(String input) {
        return "\"" + input + "\"";
    }

    public abstract String setName();

    public abstract Class<?> setType();

    public abstract String setElementFromMethod(MI method, String indent);

    /**
     * Each imported class will be added to the file in the form: <code>import <fully qualified
     * class name>;\n</code>
     */
    public abstract Set<Class<?>> importedClasses();

    // TODO should this return a class?
    public abstract Class<?> inheritedInterfaceName();

    public abstract String generateTypeBasedFields(TI typeInfo);
}
