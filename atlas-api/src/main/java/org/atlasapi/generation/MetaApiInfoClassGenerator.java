package org.atlasapi.generation;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Method;
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
import javax.tools.Diagnostic.Kind;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.content.Actor;
import org.atlasapi.content.Brand;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Certificate;
import org.atlasapi.content.Clip;
import org.atlasapi.content.ContainerRef;
import org.atlasapi.content.ContainerSummary;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentGroup;
import org.atlasapi.content.ContentGroupRef;
import org.atlasapi.content.ContentRef;
import org.atlasapi.content.CrewMember;
import org.atlasapi.content.Described;
import org.atlasapi.content.Description;
import org.atlasapi.content.Encoding;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Film;
import org.atlasapi.content.Identified;
import org.atlasapi.content.Image;
import org.atlasapi.content.Item;
import org.atlasapi.content.KeyPhrase;
import org.atlasapi.content.MediaType;
import org.atlasapi.content.Person;
import org.atlasapi.content.Policy;
import org.atlasapi.content.RelatedLink;
import org.atlasapi.content.ReleaseDate;
import org.atlasapi.content.Restriction;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.content.Subtitles;
import org.atlasapi.content.Synopses;
import org.atlasapi.content.TopicRef;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Aliased;
import org.atlasapi.entity.Identifiable;
import org.atlasapi.entity.Sourced;
import org.atlasapi.equivalence.Equivalable;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.generation.model.EndpointMethodInfo;
import org.atlasapi.generation.model.EndpointTypeInfo;
import org.atlasapi.generation.model.ModelMethodInfo;
import org.atlasapi.generation.model.ModelTypeInfo;
import org.atlasapi.generation.output.EndpointClassInfoSourceGenerator;
import org.atlasapi.generation.output.ModelClassInfoSourceGenerator;
import org.atlasapi.generation.output.SourceGenerator;
import org.atlasapi.generation.output.writers.JavaxSourceFileWriter;
import org.atlasapi.generation.output.writers.SourceFileWriter;
import org.atlasapi.generation.parsing.EndpointTypeParser;
import org.atlasapi.generation.parsing.JavadocParser;
import org.atlasapi.generation.parsing.ModelTypeParser;
import org.atlasapi.generation.parsing.StandardJavadocParser;
import org.atlasapi.generation.parsing.TypeParser;
import org.atlasapi.generation.processing.ControllerAnnotationProcessor;
import org.atlasapi.generation.processing.FieldNameProcessor;
import org.atlasapi.query.v4.channel.ChannelController;
import org.atlasapi.query.v4.channelgroup.ChannelGroupController;
import org.atlasapi.query.v4.content.ContentController;
import org.atlasapi.query.v4.schedule.ScheduleController;
import org.atlasapi.query.v4.topic.TopicController;
import org.atlasapi.schedule.ChannelSchedule;
import org.atlasapi.segment.Segment;
import org.atlasapi.segment.SegmentRef;
import org.atlasapi.topic.Topic;

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

    	// TODO can this be taken from packages instead?
    	ImmutableList<Class<?>> sourceClasses = ImmutableList.<Class<?>>of(
    			Content.class,
    			Described.class,
    			Identified.class,
    			Identifiable.class,
    			Aliased.class,
    			Sourced.class,
    			Equivalable.class,
    			ContentController.class,
    			TopicController.class,
    			ScheduleController.class,
                ChannelController.class,
                ChannelGroupController.class,
    			Clip.class,
                TopicRef.class,
                ContainerRef.class,
                Item.class,
                Certificate.class,
                Actor.class,
                Brand.class,
                ContentGroup.class,
                Description.class,  
                Encoding.class,
                Policy.class,
                Episode.class,
                Film.class,
                Image.class,
                Person.class,
                CrewMember.class,
                Restriction.class,
                RelatedLink.class,
                Synopses.class,
                Topic.class,
                ChannelSchedule.class,
                Subtitles.class,
                ReleaseDate.class,
                Alias.class,
                ContentGroupRef.class,
                ContainerSummary.class,
                Segment.class,
                SegmentRef.class,
                MediaType.class,
                EquivalenceRef.class,
                KeyPhrase.class,
                Broadcast.class,
                ContentRef.class,
                Channel.class,
                ChannelGroup.class,
                ChannelGroupMembership.class,
                SeriesRef.class
		);
    	ImmutableList<Class<?>> outputModelClasses = ImmutableList.<Class<?>>of(
    			Content.class,
    			Described.class,
    			Identified.class,
    			Clip.class,
    			TopicRef.class,
                ContainerRef.class,
                Item.class,
                Certificate.class,
                Actor.class,
                Brand.class,
                ContentGroup.class,
                Description.class,
                Encoding.class,
                Policy.class,
                Episode.class,
                Film.class,
                Image.class,
                Person.class,
                CrewMember.class,
                Restriction.class,
                RelatedLink.class,
                Synopses.class,
                Topic.class,
                ChannelSchedule.class,
                Subtitles.class,
                ReleaseDate.class,
                Alias.class,
                ContentGroupRef.class,
                Segment.class,
                SegmentRef.class,
                ContainerSummary.class,
                MediaType.class,
                EquivalenceRef.class,
                KeyPhrase.class,
                Broadcast.class,
                ContentRef.class,
                SeriesRef.class,
                Channel.class,
                ChannelGroup.class,
                ChannelGroupMembership.class
		);
    	
    	SourceFileWriter<ModelTypeInfo> modelWriter = new JavaxSourceFileWriter<ModelTypeInfo>();
		JavadocParser docParser = new StandardJavadocParser();
		TypeParser<ModelTypeInfo, ModelMethodInfo> typeParser = new ModelTypeParser(docParser, outputModelClasses);
		SourceGenerator<ModelTypeInfo, ModelMethodInfo> modelGenerator = new ModelClassInfoSourceGenerator();
		HierarchyExtractor hierarchyExtractor = new ReflectionBasedHierarchyExtractor();
		
		// TODO try running as single run with two processors rather than two runs with one processor each
		
        AbstractProcessor processor = new FieldNameProcessor(modelGenerator, hierarchyExtractor, modelWriter, typeParser, outputModelClasses);
    	boolean modelGenerationFailed = classGenerator.generateInfoClasses(processor, sourceClasses);
    	if (modelGenerationFailed) {
    		System.err.println("model info class generation failed");
    		System.exit(-1);
    	}

    	SourceFileWriter<EndpointTypeInfo> endpointWriter = new JavaxSourceFileWriter<EndpointTypeInfo>();
    	TypeParser<EndpointTypeInfo, EndpointMethodInfo> endpointTypeParser = new EndpointTypeParser(docParser);
    	SourceGenerator<EndpointTypeInfo, EndpointMethodInfo> endpointGenerator = new EndpointClassInfoSourceGenerator();
    	processor = new ControllerAnnotationProcessor(endpointGenerator, endpointWriter, endpointTypeParser, sourceClasses);
    	boolean endpointGenerationFailed = classGenerator.generateInfoClasses(processor, sourceClasses);
    	if (endpointGenerationFailed) {
    		System.err.println("endpoint info class generation failed");
    		System.exit(-1);
    	}
    }
    
	public boolean generateInfoClasses(AbstractProcessor processor, List<Class<?>> sourceClasses) throws Exception {
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
			ImmutableSet<? extends AbstractProcessor> processors) throws Exception {

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
						"-s", "./src/main/java"
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
			StandardJavaFileManager fileManager) throws Exception {
		addPath("./../atlas-core/src/main/java/");
		addPath("./src/main/java/");
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
