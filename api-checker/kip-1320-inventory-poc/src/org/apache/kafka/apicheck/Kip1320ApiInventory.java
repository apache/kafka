/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.apicheck;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.signature.SignatureReader;
import org.objectweb.asm.signature.SignatureVisitor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Local KIP-1320 research tool. It reuses KIP-1265's package-private API-surface
 * implementation by compiling into the same Java package.
 */
public final class Kip1320ApiInventory {
    private static final Pattern KAFKA_FQCN = Pattern.compile(
            "org\\.apache\\.kafka(?:\\.[A-Za-z_$][A-Za-z0-9_$]*)+");

    private static final class ClassInfo {
        final ClassFacts facts;
        final File jarPath;
        final String artifact;
        String superName;
        final List<String> interfaces = new ArrayList<>();

        ClassInfo(ClassFacts facts, File jarPath) {
            this.facts = facts;
            this.jarPath = jarPath;
            this.artifact = jarPath.getName();
        }
    }

    private static final class Usage {
        int importHits;
        int downstreamRepositories;
        String downstreamExamples = "";
        String legacyQueryUrl = "";
    }

    private enum UsageMetric {
        IMPORT_HITS("import_hits", "external source import hits") {
            @Override
            int value(Usage usage) {
                return usage.importHits;
            }
        },
        REPOSITORIES("repositories", "observed downstream repositories") {
            @Override
            int value(Usage usage) {
                return usage.downstreamRepositories;
            }
        };

        final String optionName;
        final String description;

        UsageMetric(String optionName, String description) {
            this.optionName = optionName;
            this.description = description;
        }

        abstract int value(Usage usage);

        static UsageMetric parse(String value) {
            for (UsageMetric metric : values()) {
                if (metric.optionName.equals(value)) {
                    return metric;
                }
            }
            throw new IllegalArgumentException(
                    "Unknown usage metric: " + value + ". Expected import_hits or repositories");
        }
    }

    private static final class SourceLocation {
        final String path;
        final int line;

        SourceLocation(String path, int line) {
            this.path = path;
            this.line = line;
        }

        String citation() {
            return path.isEmpty() ? "not found in scanned source tree" : path + ":" + line;
        }
    }

    private static final class SignatureEdge {
        final String target;
        final String location;
        final String position;

        SignatureEdge(String target, String location, String position) {
            this.target = target;
            this.location = location;
            this.position = position;
        }
    }

    private static final class Evidence {
        boolean signatureLeak;
        final Set<String> signatureOwners = new TreeSet<>();
        boolean directSignatureLeak;
        final Set<String> directSignatureOwners = new TreeSet<>();
        final Set<String> transitiveSignatureOwners = new TreeSet<>();
        boolean javadocGap;
        boolean publicSupertype;
        final Set<String> publicSupertypes = new TreeSet<>();
        boolean configReferenced;
        final Set<String> configPaths = new TreeSet<>();
    }

    private static final class Options {
        final List<File> jars = new ArrayList<>();
        final List<File> javadocJars = new ArrayList<>();
        Path repoRoot;
        Path usageTsv;
        String usageSourceLabel = "";
        String usageSnapshotSha256 = "";
        String scannerSha256 = "";
        String inputManifestSha256 = "";
        String legacyQueryVersion = "";
        String legacyQueryTemplate = "";
        String javaQueryVersion = "";
        String javaQueryTemplate = "";
        String scalaQueryVersion = "";
        String scalaQueryTemplate = "";
        Path outTsv;
        Path outMarkdown;
        Path outQueryManifest;
        Path outEvidenceMarkdown;
        String snapshotSha = "unknown";
        UsageMetric usageMetric = UsageMetric.IMPORT_HITS;
        int usageThreshold = 1000;
    }

    private Kip1320ApiInventory() {
    }

    public static void main(String[] args) throws Exception {
        Options options = parseOptions(args);
        ApiSurface surface = ApiSurfaceScanner.scan(options.jars);
        Map<String, ClassInfo> classes = scanClasses(options.jars);
        Map<String, Evidence> evidence = new HashMap<>();
        classes.keySet().forEach(name -> evidence.put(name, new Evidence()));

        collectSignatureLeaks(surface, classes, evidence);
        collectJavadocGaps(surface, options.javadocJars, evidence);
        collectPublicSupertypeSignals(surface, classes, evidence);
        collectConfigReferences(options.repoRoot, classes, evidence);

        Map<String, Usage> usage = readUsage(options.usageTsv);
        Map<String, SourceLocation> sourceLocations = locateSources(options.repoRoot, classes);
        writeTsv(options, surface, classes, evidence, usage, sourceLocations);
        writeMarkdown(options, surface, classes, evidence, usage);
        writeQueryManifest(options, surface, classes, sourceLocations);
        writeEvidenceMarkdown(options, surface, classes, evidence, usage, sourceLocations);
    }

    private static Options parseOptions(String[] args) {
        Options options = new Options();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--jar":
                    options.jars.add(new File(requireValue(args, ++i, "--jar")));
                    break;
                case "--javadoc-jar":
                    options.javadocJars.add(new File(requireValue(args, ++i, "--javadoc-jar")));
                    break;
                case "--repo-root":
                    options.repoRoot = Path.of(requireValue(args, ++i, "--repo-root"));
                    break;
                case "--usage-tsv":
                    options.usageTsv = Path.of(requireValue(args, ++i, "--usage-tsv"));
                    break;
                case "--usage-source-label":
                    options.usageSourceLabel = requireValue(
                            args, ++i, "--usage-source-label");
                    break;
                case "--usage-snapshot-sha256":
                    options.usageSnapshotSha256 = requireValue(
                            args, ++i, "--usage-snapshot-sha256");
                    break;
                case "--scanner-sha256":
                    options.scannerSha256 = requireValue(args, ++i, "--scanner-sha256");
                    break;
                case "--input-manifest-sha256":
                    options.inputManifestSha256 = requireValue(
                            args, ++i, "--input-manifest-sha256");
                    break;
                case "--legacy-query-version":
                    options.legacyQueryVersion = requireValue(
                            args, ++i, "--legacy-query-version");
                    break;
                case "--legacy-query-template":
                    options.legacyQueryTemplate = requireValue(
                            args, ++i, "--legacy-query-template");
                    break;
                case "--java-query-version":
                    options.javaQueryVersion = requireValue(
                            args, ++i, "--java-query-version");
                    break;
                case "--java-query-template":
                    options.javaQueryTemplate = requireValue(
                            args, ++i, "--java-query-template");
                    break;
                case "--scala-query-version":
                    options.scalaQueryVersion = requireValue(
                            args, ++i, "--scala-query-version");
                    break;
                case "--scala-query-template":
                    options.scalaQueryTemplate = requireValue(
                            args, ++i, "--scala-query-template");
                    break;
                case "--out-tsv":
                    options.outTsv = Path.of(requireValue(args, ++i, "--out-tsv"));
                    break;
                case "--out-md":
                    options.outMarkdown = Path.of(requireValue(args, ++i, "--out-md"));
                    break;
                case "--out-query-manifest":
                    options.outQueryManifest = Path.of(
                            requireValue(args, ++i, "--out-query-manifest"));
                    break;
                case "--out-evidence-md":
                    options.outEvidenceMarkdown = Path.of(
                            requireValue(args, ++i, "--out-evidence-md"));
                    break;
                case "--snapshot-sha":
                    options.snapshotSha = requireValue(args, ++i, "--snapshot-sha");
                    break;
                case "--heavy-threshold":
                    options.usageMetric = UsageMetric.REPOSITORIES;
                    options.usageThreshold = Integer.parseInt(
                            requireValue(args, ++i, "--heavy-threshold"));
                    break;
                case "--usage-metric":
                    options.usageMetric = UsageMetric.parse(
                            requireValue(args, ++i, "--usage-metric"));
                    break;
                case "--usage-threshold":
                    options.usageThreshold = Integer.parseInt(
                            requireValue(args, ++i, "--usage-threshold"));
                    break;
                default:
                    throw new IllegalArgumentException("Unknown argument: " + args[i]);
            }
        }
        if (options.jars.isEmpty() || options.outTsv == null || options.outMarkdown == null
                || options.outQueryManifest == null || options.outEvidenceMarkdown == null) {
            throw new IllegalArgumentException(
                    "Required: --jar ... --out-tsv FILE --out-md FILE"
                            + " --out-query-manifest FILE --out-evidence-md FILE");
        }
        if (options.usageThreshold < 0) {
            throw new IllegalArgumentException("Usage threshold must be non-negative");
        }
        if (options.usageTsv != null && !Files.isRegularFile(options.usageTsv)) {
            throw new IllegalArgumentException(
                    "The --usage-tsv snapshot is not readable: " + options.usageTsv);
        }
        if (options.scannerSha256.isBlank()
                || options.inputManifestSha256.isBlank()
                || options.javaQueryVersion.isBlank()
                || options.javaQueryTemplate.isBlank()
                || options.scalaQueryVersion.isBlank()
                || options.scalaQueryTemplate.isBlank()) {
            throw new IllegalArgumentException(
                    "Scanner, input-manifest, and query provenance are required");
        }
        if (options.usageTsv != null
                && (options.usageSnapshotSha256.isBlank()
                || options.legacyQueryVersion.isBlank()
                || options.legacyQueryTemplate.isBlank())) {
            throw new IllegalArgumentException(
                    "Usage provenance is required when --usage-tsv is provided");
        }
        if (options.usageTsv == null) {
            options.usageSnapshotSha256 = "none";
            options.legacyQueryVersion = "none";
        }
        for (File jar : options.jars) {
            if (!jar.isFile()) {
                throw new IllegalArgumentException("Kafka jar does not exist: " + jar);
            }
        }
        return options;
    }

    private static String requireValue(String[] args, int index, String option) {
        if (index >= args.length) {
            throw new IllegalArgumentException("Missing value for " + option);
        }
        return args[index];
    }

    private static Map<String, ClassInfo> scanClasses(List<File> jars) throws IOException {
        Map<String, ClassInfo> classes = new LinkedHashMap<>();
        for (File jarPath : jars) {
            try (JarFile jar = new JarFile(jarPath)) {
                Enumeration<JarEntry> entries = jar.entries();
                while (entries.hasMoreElements()) {
                    JarEntry entry = entries.nextElement();
                    if (!entry.getName().endsWith(".class")) {
                        continue;
                    }
                    String binaryName = entry.getName().replace('/', '.')
                            .replaceFirst("\\.class$", "");
                    if (!binaryName.startsWith("org.apache.kafka.")
                            || binaryName.endsWith("package-info")
                            || binaryName.endsWith("module-info")
                            || isSyntheticOrAnonymous(binaryName)
                            || classes.containsKey(binaryName)) {
                        continue;
                    }
                    ClassFacts facts = ApiSurfaceScanner.readClassFacts(jar, entry, binaryName);
                    ClassInfo info = new ClassInfo(facts, jarPath);
                    readHierarchy(jar, entry, info);
                    classes.put(binaryName, info);
                }
            }
        }
        return classes;
    }

    private static void readHierarchy(JarFile jar, JarEntry entry, ClassInfo info) throws IOException {
        try (InputStream input = jar.getInputStream(entry)) {
            new ClassReader(input).accept(new ClassVisitor(Opcodes.ASM9) {
                @Override
                public void visit(int version, int access, String name, String signature,
                                  String superName, String[] interfaces) {
                    if (superName != null) {
                        info.superName = superName.replace('/', '.');
                    }
                    if (interfaces != null) {
                        Arrays.stream(interfaces)
                                .map(value -> value.replace('/', '.'))
                                .forEach(info.interfaces::add);
                    }
                }
            }, ClassReader.SKIP_CODE | ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
        }
    }

    private static boolean isSyntheticOrAnonymous(String binaryName) {
        if (binaryName.contains("$$")) {
            return true;
        }
        int dollar = binaryName.indexOf('$');
        while (dollar >= 0) {
            int next = binaryName.indexOf('$', dollar + 1);
            int end = next < 0 ? binaryName.length() : next;
            if (end > dollar + 1 && Character.isDigit(binaryName.charAt(dollar + 1))) {
                return true;
            }
            dollar = next;
        }
        return false;
    }

    private static void collectSignatureLeaks(ApiSurface surface,
                                              Map<String, ClassInfo> classes,
                                              Map<String, Evidence> evidence) throws IOException {
        Deque<String> queue = new ArrayDeque<>();
        Map<String, String> reachabilityPath = new HashMap<>();
        Map<String, Integer> reachabilityDepth = new HashMap<>();
        Set<String> visited = new HashSet<>();
        List<String> publicSeeds = classes.entrySet().stream()
                .filter(entry -> surface.isEffectivelyPublic(entry.getKey()))
                .filter(entry -> entry.getValue().facts.isExternallyVisible())
                .map(Map.Entry::getKey)
                .sorted()
                .collect(Collectors.toList());
        for (String seed : publicSeeds) {
            queue.addLast(seed);
            reachabilityPath.put(seed, classes.get(seed).facts.dottedName());
            reachabilityDepth.put(seed, 0);
        }

        while (!queue.isEmpty()) {
            String owner = queue.removeFirst();
            if (!visited.add(owner)) {
                continue;
            }
            ClassInfo info = classes.get(owner);
            int ownerDepth = reachabilityDepth.getOrDefault(owner, 0);
            String entryPath = owner.replace('.', '/') + ".class";
            try (JarFile jar = new JarFile(info.jarPath)) {
                JarEntry jarEntry = jar.getJarEntry(entryPath);
                if (jarEntry == null) {
                    continue;
                }
                try (InputStream input = jar.getInputStream(jarEntry)) {
                    for (SignatureEdge edge : scanExternallyVisibleSignatures(input, owner)) {
                        ClassInfo targetInfo = classes.get(edge.target);
                        if (targetInfo == null) {
                            continue;
                        }
                        String ownerPath = reachabilityPath.getOrDefault(owner, info.facts.dottedName());
                        String step = edge.location.replace('$', '.') + " (" + edge.position + ")";
                        String path = ownerPath.equals(info.facts.dottedName())
                                ? step
                                : ownerPath + " -> " + step;
                        if (!surface.isEffectivelyPublic(edge.target)) {
                            Evidence hit = evidence.get(edge.target);
                            if (hit != null) {
                                hit.signatureLeak = true;
                                hit.signatureOwners.add(path);
                                if (ownerDepth == 0) {
                                    hit.directSignatureLeak = true;
                                    hit.directSignatureOwners.add(path);
                                } else {
                                    hit.transitiveSignatureOwners.add(path);
                                }
                            }
                        }
                        if (targetInfo.facts.isExternallyVisible()
                                && !reachabilityPath.containsKey(edge.target)) {
                            reachabilityPath.put(edge.target,
                                    path + " -> " + targetInfo.facts.dottedName());
                            reachabilityDepth.put(edge.target, ownerDepth + 1);
                            queue.addLast(edge.target);
                        }
                    }
                }
            }
        }
    }

    private static List<SignatureEdge> scanExternallyVisibleSignatures(InputStream input,
                                                                       String owner) throws IOException {
        List<SignatureEdge> edges = new ArrayList<>();
        new ClassReader(input).accept(new ClassVisitor(Opcodes.ASM9) {
            @Override
            public void visit(int version, int access, String name, String signature,
                              String superName, String[] interfaces) {
                collectInternalName(superName, owner, "SUPERTYPE", edges);
                if (interfaces != null) {
                    for (String iface : interfaces) {
                        collectInternalName(iface, owner, "SUPERTYPE", edges);
                    }
                }
                scanSignature(signature, owner, "SUPERTYPE_SIGNATURE", edges);
            }

            @Override
            public FieldVisitor visitField(int access, String name, String descriptor,
                                           String signature, Object value) {
                if ((access & (Opcodes.ACC_PUBLIC | Opcodes.ACC_PROTECTED)) == 0
                        || (access & Opcodes.ACC_SYNTHETIC) != 0) {
                    return null;
                }
                String location = owner + "#" + name;
                collectType(Type.getType(descriptor), location, "FIELD", edges);
                scanSignature(signature, location, "FIELD_SIGNATURE", edges);
                return null;
            }

            @Override
            public MethodVisitor visitMethod(int access, String name, String descriptor,
                                             String signature, String[] exceptions) {
                if ((access & (Opcodes.ACC_PUBLIC | Opcodes.ACC_PROTECTED)) == 0
                        || (access & (Opcodes.ACC_BRIDGE | Opcodes.ACC_SYNTHETIC)) != 0) {
                    return null;
                }
                String location = owner + "#" + name;
                collectType(Type.getReturnType(descriptor), location, "RETURN", edges);
                for (Type argument : Type.getArgumentTypes(descriptor)) {
                    collectType(argument, location, "PARAMETER", edges);
                }
                if (exceptions != null) {
                    for (String exception : exceptions) {
                        collectInternalName(exception, location, "EXCEPTION", edges);
                    }
                }
                scanSignature(signature, location, "GENERIC_SIGNATURE", edges);
                return null;
            }
        }, ClassReader.SKIP_CODE | ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
        return edges;
    }

    private static void scanSignature(String signature, String location, String position,
                                      List<SignatureEdge> edges) {
        if (signature == null) {
            return;
        }
        SignatureVisitor visitor = new SignatureVisitor(Opcodes.ASM9) {
            @Override
            public void visitClassType(String name) {
                collectInternalName(name, location, position, edges);
            }
        };
        SignatureReader reader = new SignatureReader(signature);
        try {
            reader.accept(visitor);
        } catch (IllegalArgumentException ignored) {
            reader.acceptType(visitor);
        }
    }

    private static void collectType(Type type, String location, String position,
                                    List<SignatureEdge> edges) {
        if (type.getSort() == Type.ARRAY) {
            collectType(type.getElementType(), location, position, edges);
        } else if (type.getSort() == Type.OBJECT) {
            collectBinaryName(type.getClassName(), location, position, edges);
        }
    }

    private static void collectInternalName(String internalName, String location, String position,
                                            List<SignatureEdge> edges) {
        if (internalName != null) {
            collectBinaryName(internalName.replace('/', '.'), location, position, edges);
        }
    }

    private static void collectBinaryName(String binaryName, String location, String position,
                                          List<SignatureEdge> edges) {
        if (binaryName.startsWith("org.apache.kafka.")) {
            edges.add(new SignatureEdge(binaryName, location, position));
        }
    }

    private static void collectJavadocGaps(ApiSurface surface, List<File> javadocJars,
                                           Map<String, Evidence> evidence) throws IOException {
        for (File javadocJar : javadocJars) {
            if (!javadocJar.isFile()) {
                continue;
            }
            CheckResult result = JavadocConsistencyValidator.validate(javadocJar, surface);
            for (PublicApiViolation violation : result.violations()) {
                if (!"MISSING_PUBLICAPI_ANNOTATION".equals(violation.getViolationType())) {
                    continue;
                }
                Evidence hit = findEvidence(evidence, violation.getClassName());
                if (hit != null) {
                    hit.javadocGap = true;
                }
            }
        }
    }

    private static void collectPublicSupertypeSignals(ApiSurface surface,
                                                       Map<String, ClassInfo> classes,
                                                       Map<String, Evidence> evidence) {
        for (Map.Entry<String, ClassInfo> entry : classes.entrySet()) {
            if (surface.isEffectivelyPublic(entry.getKey())) {
                continue;
            }
            Evidence hit = evidence.get(entry.getKey());
            Set<String> publicSupertypes = findPublicKafkaSupertypes(
                    entry.getValue(), surface, classes);
            if (!publicSupertypes.isEmpty()) {
                hit.publicSupertype = true;
                hit.publicSupertypes.addAll(publicSupertypes);
            }
        }
    }

    private static Set<String> findPublicKafkaSupertypes(ClassInfo start, ApiSurface surface,
                                                          Map<String, ClassInfo> classes) {
        Set<String> result = new TreeSet<>();
        Set<String> visited = new HashSet<>();
        Deque<String> queue = new ArrayDeque<>();
        if (start.superName != null) {
            queue.add(start.superName);
        }
        queue.addAll(start.interfaces);
        while (!queue.isEmpty()) {
            String name = queue.removeFirst();
            if (!visited.add(name) || !name.startsWith("org.apache.kafka.")) {
                continue;
            }
            if (surface.isEffectivelyPublic(name)) {
                result.add(name.replace('$', '.'));
                continue;
            }
            ClassInfo parent = classes.get(name);
            if (parent == null) {
                continue;
            }
            if (parent.superName != null) {
                queue.addLast(parent.superName);
            }
            queue.addAll(parent.interfaces);
        }
        return result;
    }

    private static void collectConfigReferences(Path repoRoot, Map<String, ClassInfo> classes,
                                                 Map<String, Evidence> evidence) throws IOException {
        if (repoRoot == null || !Files.isDirectory(repoRoot)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(repoRoot)) {
            for (Path path : paths.filter(Files::isRegularFile)
                    .filter(path -> isConfigurationEvidencePath(repoRoot, path))
                    .collect(Collectors.toList())) {
                if (Files.size(path) > 2_000_000) {
                    continue;
                }
                String text;
                try {
                    text = Files.readString(path, StandardCharsets.UTF_8);
                } catch (IOException e) {
                    continue;
                }
                Matcher matcher = KAFKA_FQCN.matcher(text);
                while (matcher.find()) {
                    String fqcn = matcher.group();
                    Evidence hit = findEvidence(evidence, fqcn);
                    if (hit == null) {
                        continue;
                    }
                    hit.configReferenced = true;
                    hit.configPaths.add(repoRoot.relativize(path).toString());
                }
            }
        }
    }

    private static boolean isConfigurationEvidencePath(Path repoRoot, Path path) {
        String relative = repoRoot.relativize(path).toString().replace(File.separatorChar, '/');
        return relative.startsWith("docs/")
                || relative.startsWith("config/")
                || relative.contains("/src/main/resources/");
    }

    private static Evidence findEvidence(Map<String, Evidence> evidence, String name) {
        Evidence direct = evidence.get(name);
        if (direct != null) {
            return direct;
        }
        if (name.indexOf('$') >= 0) {
            return evidence.get(name.replace('$', '.'));
        }
        String current = name;
        while (current.contains(".")) {
            int dot = current.lastIndexOf('.');
            current = current.substring(0, dot) + '$' + current.substring(dot + 1);
            direct = evidence.get(current);
            if (direct != null) {
                return direct;
            }
        }
        return null;
    }

    private static Map<String, SourceLocation> locateSources(
            Path repoRoot,
            Map<String, ClassInfo> classes
    ) throws IOException {
        Map<String, SourceLocation> result = new HashMap<>();
        if (repoRoot == null || !Files.isDirectory(repoRoot)) {
            return result;
        }

        Map<String, List<Path>> filesByName = new HashMap<>();
        try (Stream<Path> paths = Files.walk(repoRoot)) {
            for (Path path : paths.filter(Files::isRegularFile)
                    .filter(Kip1320ApiInventory::isMainSourcePath)
                    .collect(Collectors.toList())) {
                filesByName.computeIfAbsent(path.getFileName().toString(), ignored -> new ArrayList<>())
                        .add(path);
            }
        }
        filesByName.values().forEach(paths -> paths.sort(Comparator.comparing(Path::toString)));

        for (Map.Entry<String, ClassInfo> entry : classes.entrySet()) {
            String binaryName = entry.getKey();
            String topLevel = binaryName.contains("$")
                    ? binaryName.substring(0, binaryName.indexOf('$')) : binaryName;
            String topLevelSimpleName = topLevel.substring(topLevel.lastIndexOf('.') + 1);
            String expectedSuffix = topLevel.replace('.', '/');
            List<Path> candidates = new ArrayList<>();
            candidates.addAll(filesByName.getOrDefault(topLevelSimpleName + ".java", List.of()));
            candidates.addAll(filesByName.getOrDefault(topLevelSimpleName + ".scala", List.of()));
            candidates.sort(Comparator.comparing(Path::toString));

            Path selected = candidates.stream()
                    .filter(path -> normalizedRelativePath(repoRoot, path)
                            .matches(".*" + Pattern.quote(expectedSuffix) + "\\.(java|scala)$"))
                    .findFirst()
                    .orElse(candidates.isEmpty() ? null : candidates.get(0));
            if (selected == null) {
                result.put(binaryName, new SourceLocation("", 0));
                continue;
            }

            String simpleName = binaryName.indexOf('$') >= 0
                    ? binaryName.substring(binaryName.lastIndexOf('$') + 1)
                    : binaryName.substring(binaryName.lastIndexOf('.') + 1);
            int line = findDeclarationLine(selected, simpleName);
            result.put(binaryName, new SourceLocation(
                    normalizedRelativePath(repoRoot, selected), line));
        }
        return result;
    }

    private static boolean isMainSourcePath(Path path) {
        String normalized = path.toString().replace(File.separatorChar, '/');
        return (normalized.endsWith(".java") || normalized.endsWith(".scala"))
                && normalized.contains("/src/main/")
                && !normalized.contains("/build/");
    }

    private static String normalizedRelativePath(Path repoRoot, Path path) {
        return repoRoot.relativize(path).toString().replace(File.separatorChar, '/');
    }

    private static int findDeclarationLine(Path path, String simpleName) {
        Pattern declaration = Pattern.compile(
                "\\b(?:class|interface|enum|record)\\s+" + Pattern.quote(simpleName) + "\\b");
        try {
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
            for (int i = 0; i < lines.size(); i++) {
                if (declaration.matcher(lines.get(i)).find()) {
                    return i + 1;
                }
            }
        } catch (IOException ignored) {
            // The missing line is recorded as zero and remains visible in the evidence packet.
        }
        return 0;
    }

    private static Map<String, Usage> readUsage(Path usageTsv) throws IOException {
        Map<String, Usage> usage = new HashMap<>();
        if (usageTsv == null) {
            return usage;
        }
        if (!Files.isRegularFile(usageTsv)) {
            throw new IOException("Usage snapshot does not exist: " + usageTsv);
        }
        try (BufferedReader reader = Files.newBufferedReader(usageTsv, StandardCharsets.UTF_8)) {
            String header = reader.readLine();
            if (header == null) {
                throw new IOException("Usage snapshot is empty: " + usageTsv);
            }
            String[] columns = header.split("\\t", -1);
            Map<String, Integer> index = new HashMap<>();
            for (int i = 0; i < columns.length; i++) {
                index.put(columns[i], i);
            }
            for (String required : List.of(
                    "search_fqcn", "orig_fqcn", "import_hits", "genuine",
                    "downstream_names", "github_url")) {
                if (!index.containsKey(required)) {
                    throw new IOException(
                            "Usage snapshot is missing required column: " + required);
                }
            }
            String line;
            int lineNumber = 1;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                String[] values = line.split("\\t", -1);
                String original = value(values, index, "orig_fqcn");
                String search = value(values, index, "search_fqcn");
                if (original.isEmpty() && search.isEmpty()) {
                    throw new IOException(
                            "Usage snapshot row has no FQCN at line " + lineNumber);
                }
                Usage row = new Usage();
                row.importHits = parseNonNegativeInt(
                        value(values, index, "import_hits"), "import_hits", lineNumber);
                row.downstreamRepositories = parseNonNegativeInt(
                        value(values, index, "genuine"), "genuine", lineNumber);
                row.downstreamExamples = value(values, index, "downstream_names");
                row.legacyQueryUrl = value(values, index, "github_url");
                if (!original.isEmpty()) {
                    usage.putIfAbsent(original, row);
                }
                if (!search.isEmpty()) {
                    usage.putIfAbsent(search, row);
                }
            }
        }
        return usage;
    }

    private static String value(String[] values, Map<String, Integer> index, String name) {
        Integer position = index.get(name);
        return position == null || position >= values.length ? "" : values[position];
    }

    private static int parseNonNegativeInt(String value, String column, int lineNumber)
            throws IOException {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 0) {
                throw new NumberFormatException("negative");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IOException(
                    "Invalid " + column + " value at line " + lineNumber + ": " + value, e);
        }
    }

    private static String usageSource(Options options) {
        if (!options.usageSourceLabel.isBlank()) {
            return options.usageSourceLabel;
        }
        return options.usageTsv == null ? "none" : options.usageTsv.toString();
    }

    private static void writeTsv(Options options, ApiSurface surface,
                                 Map<String, ClassInfo> classes,
                                 Map<String, Evidence> evidence,
                                 Map<String, Usage> usage,
                                 Map<String, SourceLocation> sourceLocations) throws IOException {
        ensureParent(options.outTsv);
        List<String> lines = new ArrayList<>();
        lines.add(String.join("\t", List.of(
                "snapshot_sha", "class", "binary_name", "artifact", "source_path", "source_line",
                "candidate_non_public", "effective_public", "direct_annotation", "public_annotated",
                "public_signature_leak", "signature_leak_evidence",
                "direct_public_signature_leak", "direct_signature_leak_evidence",
                "transitive_signature_leak_evidence", "public_javadoc_gap",
                "public_supertype", "public_supertype_types",
                "config_referenced", "config_paths",
                "unreachable_internal", "already_deprecated_or_moved", "lifecycle_evidence",
                "import_hits", "observed_downstream_repositories", "downstream_examples",
                "usage_source", "usage_snapshot_sha256", "usage_query_version",
                "usage_query", "usage_query_url", "usage_query_time",
                "scanner_sha256", "input_manifest_sha256", "flags")));

        classes.entrySet().stream()
                .filter(entry -> entry.getValue().facts.isExternallyVisible())
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> lines.add(renderTsvRow(
                        options, surface, entry.getKey(), entry.getValue(),
                        evidence.get(entry.getKey()), usageFor(usage, entry.getValue().facts),
                        sourceLocations.getOrDefault(
                                entry.getKey(), new SourceLocation("", 0)))));
        Files.write(options.outTsv, lines, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private static String renderTsvRow(Options options, ApiSurface surface, String binaryName,
                                       ClassInfo info, Evidence evidence, Usage usage,
                                       SourceLocation sourceLocation) {
        boolean effectivePublic = surface.isEffectivelyPublic(binaryName);
        boolean candidate = !effectivePublic;
        boolean deprecated = surface.isDeprecated(binaryName);
        boolean inInternalPackage = isInInternalPackage(info.facts.dottedName());
        boolean lifecycleHandled = deprecated || inInternalPackage;
        boolean unreachable = candidate
                && !evidence.signatureLeak
                && !evidence.javadocGap
                && !evidence.publicSupertype
                && !evidence.configReferenced;
        String directAnnotation = info.facts.isPublic() ? "PUBLIC"
                : info.facts.isPrivate() ? "PRIVATE" : "NONE";
        List<String> flags = flags(info, evidence, unreachable, lifecycleHandled);
        String lifecycle = deprecated && inInternalPackage ? "DEPRECATED,IN_INTERNAL_PACKAGE"
                : deprecated ? "DEPRECATED"
                : inInternalPackage ? "IN_INTERNAL_PACKAGE" : "";

        return tsv(options.snapshotSha, info.facts.dottedName(), binaryName, info.artifact,
                sourceLocation.path, sourceLocation.line,
                candidate, effectivePublic, directAnnotation, info.facts.isPublic(),
                evidence.signatureLeak, join(evidence.signatureOwners),
                evidence.directSignatureLeak, join(evidence.directSignatureOwners),
                join(evidence.transitiveSignatureOwners), evidence.javadocGap,
                evidence.publicSupertype, join(evidence.publicSupertypes),
                evidence.configReferenced, joinLimited(evidence.configPaths, 8), unreachable,
                lifecycleHandled, lifecycle, usage.importHits, usage.downstreamRepositories,
                usage.downstreamExamples,
                usageSource(options),
                options.usageSnapshotSha256,
                options.legacyQueryVersion,
                queryFromTemplate(options.legacyQueryTemplate, info.facts.dottedName()),
                usage.legacyQueryUrl,
                "NOT_RECORDED_IN_LEGACY_SNAPSHOT",
                options.scannerSha256,
                options.inputManifestSha256,
                String.join(",", flags));
    }

    private static void writeMarkdown(Options options, ApiSurface surface,
                                      Map<String, ClassInfo> classes,
                                      Map<String, Evidence> evidence,
                                      Map<String, Usage> usage) throws IOException {
        ensureParent(options.outMarkdown);
        List<Map.Entry<String, ClassInfo>> candidates = classes.entrySet().stream()
                .filter(entry -> entry.getValue().facts.isExternallyVisible())
                .filter(entry -> !surface.isEffectivelyPublic(entry.getKey()))
                .collect(Collectors.toList());
        List<Map.Entry<String, ClassInfo>> thresholdMatches = candidates.stream()
                .filter(entry -> usageValue(options, usageFor(usage, entry.getValue().facts))
                        >= options.usageThreshold)
                .sorted(Comparator
                        .<Map.Entry<String, ClassInfo>>comparingInt(entry ->
                                usageValue(options, usageFor(usage, entry.getValue().facts)))
                        .reversed()
                        .thenComparing(entry -> entry.getValue().facts.dottedName()))
                .collect(Collectors.toList());
        List<Map.Entry<String, ClassInfo>> active = thresholdMatches.stream()
                .filter(entry -> !isLifecycleHandled(surface, entry))
                .collect(Collectors.toList());

        long signatureLeaks = candidates.stream().filter(entry -> evidence.get(entry.getKey()).signatureLeak).count();
        long directSignatureLeaks = candidates.stream()
                .filter(entry -> evidence.get(entry.getKey()).directSignatureLeak).count();
        long transitiveOnlySignatureLeaks = candidates.stream()
                .filter(entry -> evidence.get(entry.getKey()).signatureLeak)
                .filter(entry -> !evidence.get(entry.getKey()).directSignatureLeak).count();
        long javadocGaps = candidates.stream().filter(entry -> evidence.get(entry.getKey()).javadocGap).count();
        long publicSupertypes = candidates.stream()
                .filter(entry -> evidence.get(entry.getKey()).publicSupertype).count();
        long configReferences = candidates.stream().filter(entry -> evidence.get(entry.getKey()).configReferenced).count();
        long unreachable = candidates.stream().filter(entry -> {
            Evidence e = evidence.get(entry.getKey());
            return !e.signatureLeak && !e.javadocGap
                    && !e.publicSupertype && !e.configReferenced;
        }).count();

        List<String> out = new ArrayList<>();
        out.add("# KIP-1320 API inventory PoC");
        out.add("");
        out.add("- Kafka snapshot: `" + options.snapshotSha + "`");
        out.add("- Input manifest SHA-256: `" + options.inputManifestSha256 + "`");
        out.add("- Scanner SHA-256: `" + options.scannerSha256 + "`");
        out.add("- Legacy usage snapshot SHA-256: `" + options.usageSnapshotSha256 + "`");
        out.add("- Legacy usage query rule: `" + options.legacyQueryVersion + "`");
        out.add("- Java query manifest rule: `" + options.javaQueryVersion + "`");
        out.add("- Scala query manifest rule: `" + options.scalaQueryVersion + "`");
        out.add("- Scanned artifacts: " + options.jars.size());
        out.add("- Externally visible, effectively non-`@Public` candidates: " + candidates.size());
        out.add("- Usage metric: `" + options.usageMetric.optionName + "` ("
                + options.usageMetric.description + ")");
        out.add("- Discussion threshold: at least " + options.usageThreshold);
        out.add("- Threshold matches before lifecycle filtering: " + thresholdMatches.size());
        out.add("- Active discussion candidates: " + active.size());
        out.add("");
        out.add("## Evidence summary");
        out.add("");
        out.add("| Signal | Candidate count |");
        out.add("|---|---:|");
        out.add("| `PUBLIC_SIGNATURE_LEAK` | " + signatureLeaks + " |");
        out.add("| `PUBLIC_SIGNATURE_LEAK_DIRECT` | " + directSignatureLeaks + " |");
        out.add("| `PUBLIC_SIGNATURE_LEAK_TRANSITIVE_ONLY` | "
                + transitiveOnlySignatureLeaks + " |");
        out.add("| `PUBLIC_JAVADOC_GAP` | " + javadocGaps + " |");
        out.add("| `PUBLIC_SUPERTYPE` | " + publicSupertypes + " |");
        out.add("| `CONFIG_REFERENCED` | " + configReferences + " |");
        out.add("| `UNREACHABLE_INTERNAL` | " + unreachable + " |");
        out.add("");
        out.add("## Active discussion shortlist");
        out.add("");
        out.add("| Class | Metric value | Repositories | Import hits | Evidence flags | Detail |");
        out.add("|---|---:|---:|---:|---|---|");
        for (Map.Entry<String, ClassInfo> entry : active) {
            ClassInfo info = entry.getValue();
            Evidence e = evidence.get(entry.getKey());
            Usage u = usageFor(usage, info.facts);
            boolean lifecycleHandled = surface.isDeprecated(entry.getKey())
                    || isInInternalPackage(info.facts.dottedName());
            boolean noContractSignal = !e.signatureLeak && !e.javadocGap
                    && !e.publicSupertype && !e.configReferenced;
            List<String> flags = flags(info, e, noContractSignal, lifecycleHandled);
            List<String> details = new ArrayList<>();
            details.addAll(limit(e.directSignatureOwners, 2));
            if (details.size() < 2) {
                details.addAll(limit(e.transitiveSignatureOwners, 2 - details.size()));
            }
            details.addAll(limit(e.publicSupertypes, 2));
            details.addAll(limit(e.configPaths, 2));
            out.add("| `" + escapeMarkdown(info.facts.dottedName()) + "` | "
                    + usageValue(options, u) + " | " + u.downstreamRepositories + " | "
                    + u.importHits + " | `"
                    + String.join("`, `", flags) + "` | "
                    + escapeMarkdown(String.join("; ", details)) + " |");
        }
        out.add("");
        out.add("## Interpretation limits");
        out.add("");
        out.add("- The flags are evidence, not an automatic Public/Internal decision.");
        out.add("- `PUBLIC_SUPERTYPE` means the class reaches an effectively Public Kafka supertype;"
                + " it does not prove that the class is an SPI implementation or supported API.");
        out.add("- `CONFIG_REFERENCED` is conservative: it only scans `docs/`, `config/`, and"
                + " `src/main/resources/` for exact Kafka FQCNs.");
        out.add("- Configuration-only usage is context evidence and is not included in the"
                + " external source-code usage threshold.");
        out.add("- `IN_INTERNAL_PACKAGE` proves the current location, not that git history contains a move.");
        out.add("- Repository counts come from the saved fullscan and retain its GitHub indexing,"
                + " first-100-results, and heuristic-denylist limitations.");

        Files.write(options.outMarkdown, out, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private static void writeQueryManifest(
            Options options,
            ApiSurface surface,
            Map<String, ClassInfo> classes,
            Map<String, SourceLocation> sourceLocations
    ) throws IOException {
        ensureParent(options.outQueryManifest);
        List<String> lines = new ArrayList<>();
        lines.add(String.join("\t", List.of(
                "snapshot_sha", "input_manifest_sha256", "scanner_sha256",
                "class", "binary_name", "artifact", "source_path",
                "source_line", "query_version", "query_kind", "query")));

        classes.entrySet().stream()
                .filter(entry -> entry.getValue().facts.isExternallyVisible())
                .filter(entry -> !surface.isEffectivelyPublic(entry.getKey()))
                .filter(entry -> !isLifecycleHandled(surface, entry))
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    ClassInfo info = entry.getValue();
                    SourceLocation location = sourceLocations.getOrDefault(
                            entry.getKey(), new SourceLocation("", 0));
                    lines.add(tsv(options.snapshotSha, options.inputManifestSha256,
                            options.scannerSha256,
                            info.facts.dottedName(), entry.getKey(),
                            info.artifact, location.path, location.line,
                            options.javaQueryVersion, "java_exact_import",
                            queryFromTemplate(
                                    options.javaQueryTemplate, info.facts.dottedName())));
                    lines.add(tsv(options.snapshotSha, options.inputManifestSha256,
                            options.scannerSha256,
                            info.facts.dottedName(), entry.getKey(),
                            info.artifact, location.path, location.line,
                            options.scalaQueryVersion, "scala_exact_import",
                            queryFromTemplate(
                                    options.scalaQueryTemplate, info.facts.dottedName())));
                });

        Files.write(options.outQueryManifest, lines, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private static String queryFromTemplate(String template, String fqcn) {
        return template.replace("{FQCN}", fqcn);
    }

    private static void writeEvidenceMarkdown(
            Options options,
            ApiSurface surface,
            Map<String, ClassInfo> classes,
            Map<String, Evidence> evidence,
            Map<String, Usage> usage,
            Map<String, SourceLocation> sourceLocations
    ) throws IOException {
        ensureParent(options.outEvidenceMarkdown);
        List<Map.Entry<String, ClassInfo>> active = classes.entrySet().stream()
                .filter(entry -> entry.getValue().facts.isExternallyVisible())
                .filter(entry -> !surface.isEffectivelyPublic(entry.getKey()))
                .filter(entry -> !isLifecycleHandled(surface, entry))
                .filter(entry -> usageValue(options, usageFor(usage, entry.getValue().facts))
                        >= options.usageThreshold)
                .sorted(Comparator
                        .<Map.Entry<String, ClassInfo>>comparingInt(entry ->
                                usageValue(options, usageFor(usage, entry.getValue().facts)))
                        .reversed()
                        .thenComparing(entry -> entry.getValue().facts.dottedName()))
                .collect(Collectors.toList());

        List<String> out = new ArrayList<>();
        out.add("# KIP-1320 classification evidence packets");
        out.add("");
        out.add("- Kafka snapshot: `" + options.snapshotSha + "`");
        out.add("- Input manifest SHA-256: `" + options.inputManifestSha256 + "`");
        out.add("- Scanner SHA-256: `" + options.scannerSha256 + "`");
        out.add("- Usage source: `" + usageSource(options) + "`");
        out.add("- Usage snapshot SHA-256: `" + options.usageSnapshotSha256 + "`");
        out.add("- Usage query time: `NOT_RECORDED_IN_LEGACY_SNAPSHOT`");
        out.add("- Usage metric: `" + options.usageMetric.optionName + "`");
        out.add("- Usage threshold: " + options.usageThreshold);
        out.add("- Legacy usage query rule: `" + options.legacyQueryVersion + "`");
        out.add("- Java query manifest rule: `" + options.javaQueryVersion + "`");
        out.add("- Scala query manifest rule: `" + options.scalaQueryVersion + "`");
        out.add("- Active packets: " + active.size());
        out.add("");
        out.add("Every packet is evidence for review, not an automatic API decision.");

        for (Map.Entry<String, ClassInfo> entry : active) {
            String binaryName = entry.getKey();
            ClassInfo info = entry.getValue();
            Evidence item = evidence.get(binaryName);
            Usage itemUsage = usageFor(usage, info.facts);
            SourceLocation location = sourceLocations.getOrDefault(
                    binaryName, new SourceLocation("", 0));
            boolean unreachable = !item.signatureLeak && !item.javadocGap
                    && !item.publicSupertype && !item.configReferenced;
            List<String> itemFlags = flags(info, item, unreachable, false);

            out.add("");
            out.add("## `" + info.facts.dottedName() + "`");
            out.add("");
            out.add("### Identity");
            out.add("");
            out.add("- Artifact: `" + info.artifact + "`");
            out.add("- Source: `" + location.citation() + "`");
            out.add("- Direct audience annotation: `" + directAnnotation(info) + "`");
            out.add("- Effective audience: `PRIVATE`");
            out.add("- Evidence flags: " + markdownCodeList(itemFlags));
            out.add("- Lifecycle: `ACTIVE_NOT_DEPRECATED_NOT_INTERNAL_PACKAGE`");
            out.add("");
            out.add("### External source-code usage");
            out.add("");
            out.add("- Import hits: " + itemUsage.importHits);
            out.add("- Observed downstream repositories: " + itemUsage.downstreamRepositories);
            out.add("- Repository examples: "
                    + (itemUsage.downstreamExamples.isEmpty()
                    ? "none saved" : "`" + itemUsage.downstreamExamples + "`"));
            out.add("- Legacy query version: `" + options.legacyQueryVersion + "`");
            out.add("- Legacy query: `"
                    + queryFromTemplate(
                    options.legacyQueryTemplate, info.facts.dottedName()) + "`");
            out.add("- Saved legacy query URL: "
                    + (itemUsage.legacyQueryUrl.isEmpty()
                    ? "none saved" : "`" + itemUsage.legacyQueryUrl + "`"));
            out.add("- Query time: `NOT_RECORDED_IN_LEGACY_SNAPSHOT`");
            out.add("- Next Java query (not the source of the counts above): `"
                    + queryFromTemplate(
                    options.javaQueryTemplate, info.facts.dottedName()) + "`");
            out.add("- Next Scala query (not the source of the counts above): `"
                    + queryFromTemplate(
                    options.scalaQueryTemplate, info.facts.dottedName()) + "`");
            out.add("- Limitation: configuration-only references are not usage; the legacy"
                    + " snapshot used an exact-import text query without a language or fork"
                    + " filter and is not a complete census.");
            out.add("");
            out.add("### Contract evidence");
            out.add("");
            appendEvidence(out, "Canonical direct Public signature paths",
                    item.directSignatureOwners, 3);
            appendEvidence(out, "Canonical transitive signature paths",
                    item.transitiveSignatureOwners, 3);
            appendEvidence(out, "Public Kafka supertypes", item.publicSupertypes, 3);
            appendEvidence(out, "Configuration references (context only)", item.configPaths, 3);
            out.add("- Public Javadoc gap: `" + item.javadocGap + "`");
            out.add("");
            out.add("### Decision record");
            out.add("");
            out.add("- Proposed label: `UNCLASSIFIED`");
            out.add("- Confidence: `UNSET`");
            out.add("- Decision evidence: pending investigator, adversarial review, and verification");
            out.add("- Evidence against decision: pending");
            out.add("- Maintainer question: pending");
        }

        Files.write(options.outEvidenceMarkdown, out, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private static void appendEvidence(List<String> out, String label, Set<String> values, int limit) {
        if (values.isEmpty()) {
            out.add("- " + label + ": none observed");
            return;
        }
        out.add("- " + label + " (showing " + Math.min(values.size(), limit)
                + " of " + values.size() + "):");
        values.stream().limit(limit).forEach(value -> out.add("  - `" + value + "`"));
    }

    private static String directAnnotation(ClassInfo info) {
        return info.facts.isPublic() ? "PUBLIC" : info.facts.isPrivate() ? "PRIVATE" : "NONE";
    }

    private static String markdownCodeList(List<String> values) {
        return values.isEmpty() ? "none" : values.stream()
                .map(value -> "`" + value + "`")
                .collect(Collectors.joining(", "));
    }

    private static int usageValue(Options options, Usage usage) {
        return options.usageMetric.value(usage);
    }

    private static boolean isLifecycleHandled(
            ApiSurface surface,
            Map.Entry<String, ClassInfo> entry
    ) {
        return surface.isDeprecated(entry.getKey())
                || isInInternalPackage(entry.getValue().facts.dottedName());
    }

    private static List<String> flags(ClassInfo info, Evidence evidence, boolean unreachable,
                                      boolean lifecycleHandled) {
        List<String> flags = new ArrayList<>();
        if (info.facts.isPublic()) flags.add("PUBLIC_ANNOTATED");
        if (evidence.signatureLeak) flags.add("PUBLIC_SIGNATURE_LEAK");
        if (evidence.directSignatureLeak) flags.add("PUBLIC_SIGNATURE_LEAK_DIRECT");
        if (!evidence.transitiveSignatureOwners.isEmpty()) {
            flags.add("PUBLIC_SIGNATURE_LEAK_TRANSITIVE");
        }
        if (evidence.javadocGap) flags.add("PUBLIC_JAVADOC_GAP");
        if (evidence.publicSupertype) flags.add("PUBLIC_SUPERTYPE");
        if (evidence.configReferenced) flags.add("CONFIG_REFERENCED");
        if (unreachable) flags.add("UNREACHABLE_INTERNAL");
        if (lifecycleHandled) flags.add("ALREADY_DEPRECATED_OR_MOVED");
        return flags;
    }

    private static Usage usageFor(Map<String, Usage> usage, ClassFacts facts) {
        Usage result = usage.get(facts.dottedName());
        if (result == null) {
            result = usage.get(facts.binaryName());
        }
        return result == null ? new Usage() : result;
    }

    private static boolean isInInternalPackage(String name) {
        String lower = name.toLowerCase(Locale.ROOT);
        return lower.contains(".internal.") || lower.contains(".internals.");
    }

    private static List<String> limit(Set<String> values, int limit) {
        return values.stream().limit(limit).collect(Collectors.toList());
    }

    private static String join(Set<String> values) {
        return String.join(";", values);
    }

    private static String joinLimited(Set<String> values, int limit) {
        return values.stream().limit(limit).collect(Collectors.joining(";"));
    }

    private static void ensureParent(Path path) throws IOException {
        Path parent = path.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
    }

    private static String tsv(Object... values) {
        return Arrays.stream(values)
                .map(String::valueOf)
                .map(value -> value.replace('\t', ' ').replace('\n', ' ').replace('\r', ' '))
                .collect(Collectors.joining("\t"));
    }

    private static String escapeMarkdown(String value) {
        return value.replace("|", "\\|").replace("\n", " ");
    }
}
