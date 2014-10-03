package org.atlasapi.generation;

import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import javax.annotation.processing.AbstractProcessor;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import javax.tools.Diagnostic.Kind;
import javax.tools.JavaCompiler.CompilationTask;

import org.atlasapi.content.Content;
import org.atlasapi.content.Described;
import org.atlasapi.content.Identified;
import org.atlasapi.entity.Aliased;
import org.atlasapi.entity.Identifiable;
import org.atlasapi.query.v4.content.ContentController;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;

public class MetaApiInfoClassGenerator {

	private static final Function<Class<?>, File> CLASS_TO_FILE = new Function<Class<?>, File>() {
		@Override
		public File apply(Class<?> input) {
			String path = input.getName().replace(".", "/") + JAVA;
			try {
				return new File(Resources.getResource(path).toURI());
			} catch (URISyntaxException e) {
				throw new RuntimeException(e);
			}
		}
	};

	private static final String JAVA = ".java";
	private static final Locale DEFAULT_LOCALE = null;
	private static final Charset DEFAULT_CHARSET = null;

	public MetaApiInfoClassGenerator() {
	}


    public static void main(String[] args) throws Exception {
    	MetaApiInfoClassGenerator classGenerator = new MetaApiInfoClassGenerator();

    	ImmutableList<Class<?>> sourceClasses = ImmutableList.<Class<?>>of(
    			Content.class,
    			Described.class,
    			Identified.class,
    			Identifiable.class,
    			Aliased.class,
    			ContentController.class
		);
    	ImmutableList<Class<?>> outputModelClasses = ImmutableList.<Class<?>>of(
    			Content.class,
    			Described.class,
    			Identified.class
		);
    	
//    	addPath("file:///Users/oli/Documents/Code/atlas-deer/atlas-core/");

    	// TODO try running as single run with two processors rather than two runs with one processor each
    	//TODO rename file generator, extract writing from file creation from model parsing 
    	FileGenerator generator = new ModelClassInfoFileGenerator(outputModelClasses);
    	AbstractProcessor processor = new FieldNameAnnotationProcessor(generator);
    	boolean modelGenerationSucceeded = classGenerator.generateInfoClasses(processor, sourceClasses);
    	if (!modelGenerationSucceeded) {
    		System.err.println("model info class generation failed");
    		System.exit(-1);
    	}

    	generator = new EndpointClassInfoFileGenerator();
    	processor = new ControllerAnnotationProcessor(generator);
    	boolean endpointGenerationSucceeded = classGenerator.generateInfoClasses(processor, sourceClasses);
    	if (!endpointGenerationSucceeded) {
    		System.err.println("endpoint info class generation failed");
    		System.exit(-1);
    	}
    }
    
	private boolean generateInfoClasses(AbstractProcessor processor, List<Class<?>> sourceClasses) {
		ImmutableSet<? extends AbstractProcessor> processors = ImmutableSet.of(processor);

		List<Diagnostic<? extends JavaFileObject>> diagnostics = compileWithProcessors(sourceClasses, processors);

		boolean failed = false;
		for (Diagnostic<?> diagnostic : diagnostics) {
			failed = failed | (diagnostic.getKind().equals(Kind.ERROR));
			System.out.println(String.format("[%s] %s [%s,%s]", 
					diagnostic.getKind(), diagnostic.getMessage(Locale.getDefault()), 
					diagnostic.getLineNumber(), diagnostic.getColumnNumber()));
		}
		return failed;
	}

	private List<Diagnostic<? extends JavaFileObject>> compileWithProcessors(Iterable<Class<?>> classes, 
			ImmutableSet<? extends AbstractProcessor> processors) {

		JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
		
		DiagnosticCollector<JavaFileObject> diagnosticCollector = new DiagnosticCollector<JavaFileObject>();

		StandardJavaFileManager fileManager = compiler.getStandardFileManager(
				diagnosticCollector, 
				DEFAULT_LOCALE, 
				DEFAULT_CHARSET
		);

		Iterable<? extends JavaFileObject> sourceCompilationUnits = transformToCompilationUnits(classes, 
				fileManager);

		CompilationTask task = compiler.getTask(new OutputStreamWriter(System.out), 
				fileManager, diagnosticCollector, 
				Arrays.asList(
						"-proc:only", 
						"-s", "/Users/oli/Documents/Code/atlas-deer/atlas-api/build/generated-sources"
				), 
				null, 
				sourceCompilationUnits
		);

		task.setProcessors(processors);
		task.call();

		try {
			fileManager.close();
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}

		return diagnosticCollector.getDiagnostics();
	}


	private Iterable<? extends JavaFileObject> transformToCompilationUnits(Iterable<Class<?>> classes, 
			StandardJavaFileManager fileManager) {
		Iterable<File> sourceCompilationFiles = Iterables.transform(classes, CLASS_TO_FILE);
		
		return fileManager.getJavaFileObjectsFromFiles(sourceCompilationFiles);
	}
	
	public static void addPath(String s) throws Exception {
	    File f = new File(s);
	    URI u = f.toURI();
	    URLClassLoader urlClassLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
	    Class<URLClassLoader> urlClass = URLClassLoader.class;
	    Method method = urlClass.getDeclaredMethod("addURL", new Class[]{URL.class});
	    method.setAccessible(true);
	    method.invoke(urlClassLoader, new Object[]{u.toURL()});
	}
}
