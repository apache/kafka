package org.apache.kafka.publicapi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Core logic for checking @PublicApi annotation compliance.
 */
public class PublicApiChecker {
    public static final Logger logger = LoggerFactory.getLogger(PublicApiChecker.class);
    private static final String PUBLIC_API = "org.apache.kafka.common.annotation.PublicApi";
    private static final String DEPRECATED_ANNOTATION = "java.lang.Deprecated";

    private final ClassLoader classLoader;

    public PublicApiChecker(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    /**
     * Check public API violations using dual validation approach.
     * This method combines HTML-based class discovery from javadoc JAR with @PublicApi annotation scanning.
     */
    public List<PublicApiViolation> checkPublicApiConsistency(File javadocJar, List<File> projectJars) throws IOException {
        List<PublicApiViolation> violations = new ArrayList<>();


        Set<String> classesWithPublicDoc = findClassesFromJavadocHtml(javadocJar);
        Set<String> publicApiAnnotatedClasses = findPublicApiAnnotatedClasses(projectJars);

        violations.addAll(crossValidateClassSets(classesWithPublicDoc, publicApiAnnotatedClasses));

        for (String className : publicApiAnnotatedClasses) {
             violations.addAll(checkClassForPublicApiConsistency(className));
        }

        return violations;
    }


    /**
     * Check public API violations in source files (for external usage checking).
     */
    public List<PublicApiViolation> checkSourceFiles(List<File> sourceFiles) throws IOException {
        List<PublicApiViolation> violations = new ArrayList<>();

        for (File sourceFile : sourceFiles) {
            if (sourceFile.getName().endsWith(".java")) {
                violations.addAll(checkSourceFileForInternalApiUsage(sourceFile));
            }
        }

        return violations;
    }

    /**
     * Find class names from HTML files in a javadoc JAR.
     */
    private Set<String> findClassesFromJavadocHtml(File javadocJar) throws IOException {
        Set<String> classes = new HashSet<>();

        try (JarFile jar = new JarFile(javadocJar)) {
            Enumeration<JarEntry> entries = jar.entries();

            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();

                if (isClassHtmlFile(entry.getName())) {
                    String className = convertHtmlPathToClassName(entry.getName());
                    if (shouldCheckClass(className)) {
                        classes.add(className);
                    }
                }
            }
        }

        return classes;
    }

    /**
     * Find all classes with @PublicApi annotations by scanning project JAR files.
     */
    private Set<String> findPublicApiAnnotatedClasses(List<File> projectJars) throws IOException {
        Set<String> annotatedClasses = new HashSet<>();

        for (File jar : projectJars) {
            try (JarFile jarFile = new JarFile(jar)) {
                Enumeration<JarEntry> entries = jarFile.entries();

                while (entries.hasMoreElements()) {
                    JarEntry entry = entries.nextElement();

                    if (entry.getName().endsWith(".class") && !entry.getName().contains("$")) {
                        String className = entry.getName()
                            .replace('/', '.')
                            .replaceAll(".class$", "");
                        logger.trace("Checking class " + entry.getName() + " --> " + className);

                        if (shouldCheckClass(className) && hasPublicApiAnnotation(className)) {
                            annotatedClasses.add(className);
                        }
                    }
                }
            }
        }

        return annotatedClasses;
    }

    /**
     * Cross-validate consistency between HTML classes and @PublicApi annotated classes.
     */
    private List<PublicApiViolation> crossValidateClassSets(Set<String> classesWithPublicDoc, Set<String> annotatedClasses) {
        List<PublicApiViolation> violations = new ArrayList<>();

        // Check: @PublicApi classes missing from javadoc
        for (String className : annotatedClasses) {
            if (!classesWithPublicDoc.contains(className)) {
                violations.add(new PublicApiViolation(
                    className,
                    "MISSING_JAVADOC",
                    "Class has @PublicApi annotation but is missing from javadoc",
                    null
                ));
            }
        }

        // Check: Javadoc classes missing @PublicApi annotation
        for (String className : classesWithPublicDoc) {
            if (!annotatedClasses.contains(className)) {
                violations.add(new PublicApiViolation(
                    className,
                    "MISSING_PUBLICAPI_ANNOTATION",
                    "Class appears in javadoc but lacks @PublicApi annotation",
                    null
                ));
            }
        }

        return violations;
    }

    private List<PublicApiViolation> checkClassForPublicApiConsistency(String className) {
        List<PublicApiViolation> violations = new ArrayList<>();

        try {
            Class<?> clazz = classLoader.loadClass(className);

            violations.addAll(checkPublicMethods(clazz));
            violations.addAll(checkInnerClasses(clazz));

        } catch (ClassNotFoundException e) {
            violations.add(new PublicApiViolation(
                className,
                "CLASS_LOAD_ERROR",
                "Unable to load class: " + e.getMessage(),
                null
            ));
        }

        return violations;
    }

    private List<PublicApiViolation> checkSourceFileForInternalApiUsage(File sourceFile) throws IOException {
        List<PublicApiViolation> violations = new ArrayList<>();

        // Parse Java file for imports from org.apache.kafka.**
        try (java.util.Scanner scanner = new java.util.Scanner(sourceFile)) {
            int lineNumber = 0;
            while (scanner.hasNextLine()) {
                lineNumber++;
                String line = scanner.nextLine().trim();

                if (line.startsWith("import ") && line.contains("org.apache.kafka.")) {
                    String importedClass = extractImportedClass(line);
                    if (importedClass != null && shouldCheckClass(importedClass)) {
                        try {
                            Class<?> clazz = classLoader.loadClass(importedClass);
                            if (!hasPublicApiAnnotation(clazz)) {
                                violations.add(new PublicApiViolation(
                                    importedClass,
                                    "INTERNAL_API_USAGE",
                                    String.format("Importing internal Kafka API class at %s:%d ", sourceFile, lineNumber),
                                    null
                                ));
                            }
                        } catch (ClassNotFoundException e) {
                            // Class not found, might be using older Kafka version
                            violations.add(new PublicApiViolation(
                                importedClass,
                                "CLASS_NOT_FOUND",
                                "Unable to verify API status, class not found: " + e.getMessage(),
                                null
                            ));
                        }
                    }
                }
            }
        }

        return violations;
    }

    private List<PublicApiViolation> checkPublicMethods(Class<?> clazz) {
        List<PublicApiViolation> violations = new ArrayList<>();

        for (Method method : clazz.getDeclaredMethods()) {
            if (java.lang.reflect.Modifier.isPublic(method.getModifiers())) {
                // Check return type
                Class<?> returnType = method.getReturnType();
                if (shouldCheckClass(returnType.getName()) && !hasPublicApiAnnotation(returnType)) {
                    violations.add(new PublicApiViolation(
                            clazz.getName(),
                            "INVALID_RETURN_TYPE",
                            "Public method returns non-public API type: " + returnType.getName(),
                            method.getName()
                    ));
                }

                // Check parameters
                for (Parameter param : method.getParameters()) {
                    Class<?> paramType = param.getType();
                    if (shouldCheckClass(paramType.getName()) && !hasPublicApiAnnotation(paramType)) {
                        violations.add(new PublicApiViolation(
                                clazz.getName(),
                                "INVALID_PARAMETER_TYPE",
                                "Public method has non-public API parameter type: " + paramType.getName(),
                                method.getName()
                        ));
                    }
                }

                // Check declared exceptions
                for (Class<?> exception : method.getExceptionTypes()) {
                    if (shouldCheckClass(exception.getName()) && !hasPublicApiAnnotation(exception)) {
                        violations.add(new PublicApiViolation(
                                clazz.getName(),
                                "INVALID_EXCEPTION_TYPE",
                                "Public method declares non-public API exception type: " + exception.getName(),
                                method.getName()
                        ));
                    }
                }

            }
        }

        return violations;
    }

    private List<PublicApiViolation> checkInnerClasses(Class<?> clazz) {
        List<PublicApiViolation> violations = new ArrayList<>();

        for (Class<?> innerClass : clazz.getDeclaredClasses()) {
            if (java.lang.reflect.Modifier.isPublic(innerClass.getModifiers())) {
                if (!hasPublicApiAnnotation(innerClass)) {
                    violations.add(new PublicApiViolation(
                        clazz.getName(),
                        "INVALID_INNER_CLASS",
                        "Public inner class lacks @PublicApi annotation: " + innerClass.getSimpleName(),
                        innerClass.getSimpleName()
                    ));
                }
            }
        }

        return violations;
    }

    private boolean hasPublicApiAnnotation(Class<?> clazz) {
        if (logger.isDebugEnabled()) {
            logger.debug("Checking @PublicApi annotation for class: {} with annotations: {}", clazz.getName(),
                    Arrays.stream(clazz.getDeclaredAnnotations())
                            .map(a -> a.annotationType().getName())
                            .reduce((a, b) -> a + ", " + b)
                            .orElse("No Annotations"));
        }
        for (Annotation declaredAnnotation : clazz.getDeclaredAnnotations()) {
            if (declaredAnnotation.annotationType().getName().equals(PUBLIC_API)) {
                return true;
            }
        }
        return false;
    }


    private boolean shouldCheckClass(String className) {
        logger.debug("Deciding whether to check class: {}", className);
        // Skip if not a Kafka class
        if (!className.startsWith("org.apache.kafka.")) {
            return false;
        }

        // Skip if it's a deprecated class
        try {
            Class<?> clazz = classLoader.loadClass(className);
            if (clazz.getAnnotation(Deprecated.class) != null) {
                return false;
            }
        } catch (ClassNotFoundException e) {
            // If class cannot be loaded, we cannot check deprecation, proceed with other checks
        }

        return true;
    }

    private String extractImportedClass(String importLine) {
        // Extract class name from import statement
        String line = importLine.trim();
        if (line.startsWith("import ") && line.endsWith(";")) {
            return line.substring(7, line.length() - 1).trim();
        }
        return null;
    }

    /**
     * Check if an entry path represents a class HTML file in javadoc.
     */
    private boolean isClassHtmlFile(String path) {
        // Must end with .html
        if (!path.endsWith(".html")) {
            return false;
        }

        if (!path.startsWith("org/apache/kafka/")) {
            return false;
        }

        String fileName = path.substring(path.lastIndexOf('/') + 1);

        // Must represent a class (starts with uppercase letter)
        String classNamePart = fileName.replaceAll(".html$", "");
        if (classNamePart.isEmpty() || !Character.isUpperCase(classNamePart.charAt(0))) {
            return false;
        }

        return true;
    }

    /**
     * Convert HTML file path to class name.
     * Example: "org/apache/kafka/common/resource/Resource.html" -> "org.apache.kafka.common.resource.Resource"
     */
    private String convertHtmlPathToClassName(String htmlPath) {
        return htmlPath.replace('/', '.')
                      .replaceAll(".html$", "");
    }

    /**
     * Check if a class (by name) has @PublicApi annotation.
     */
    private boolean hasPublicApiAnnotation(String className) {
        try {
            Class<?> clazz = classLoader.loadClass(className);
            boolean hasPublicApiAnnotation = hasPublicApiAnnotation(clazz);
            logger.trace("Class {} has @PublicApi: {}", className, hasPublicApiAnnotation);
            return hasPublicApiAnnotation;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create a ClassLoader that includes the specified JAR files.
     */
    public static ClassLoader createClassLoader(List<File> jarFiles) throws IOException {
        URL[] urls = new URL[jarFiles.size()];
        for (int i = 0; i < jarFiles.size(); i++) {
            urls[i] = jarFiles.get(i).toURI().toURL();
        }
        return new URLClassLoader(urls, PublicApiChecker.class.getClassLoader());
    }
}