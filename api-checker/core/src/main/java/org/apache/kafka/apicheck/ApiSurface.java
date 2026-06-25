/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * The Kafka public-API surface, resolved from one project-jars scan. Immutable; consumed by the
 * cascade and javadoc validators. Built via {@link ApiSurfaceScanner}.
 *
 * <p>All lookup methods accept a class name in either binary ({@code Outer$Inner}) or dotted
 * ({@code Outer.Inner}) form — callers don't need to know which form the surface stores. The two
 * iteration sets ({@link #effectivePublic()} and {@link #directPublic()}) return {@link ClassFacts}
 * directly so callers never juggle name strings.
 */
final class ApiSurface {

    /** Every class that is effectively {@code @Public} (direct or inherited), regardless of visibility. */
    private final Set<ClassFacts> effectivePublic;
    /** Classes carrying a direct {@code @InterfaceAudience.Public} — drives the MISSING_JAVADOC iteration. */
    private final Set<ClassFacts> directPublic;
    private final Map<String, ClassFacts> byDottedName;
    private final Map<String, File> jarByDottedName;

    private ApiSurface(Builder b) {
        this.effectivePublic = Set.copyOf(b.effectivePublic);
        this.directPublic = Set.copyOf(b.directPublic);
        this.byDottedName = Map.copyOf(b.byDottedName);
        this.jarByDottedName = Map.copyOf(b.jarByDottedName);
    }

    /**
     * Every class that is effectively {@code @Public} (direct or inherited). Cascade iteration
     * filters this further on {@link ClassFacts#isExternallyVisible()} — private/package-private
     * nested classes inherit the audience but their methods aren't reachable to consumers.
     */
    Set<ClassFacts> effectivePublic() {
        return effectivePublic;
    }

    /** Classes carrying a *direct* {@code @InterfaceAudience.Public}. Drives the MISSING_JAVADOC check. */
    Set<ClassFacts> directPublic() {
        return directPublic;
    }

    /** Look up facts by either binary or dotted name. Returns {@code null} if not in any scanned jar. */
    ClassFacts factsOf(String name) {
        return byDottedName.get(normalize(name));
    }

    /** @return the jar that contained the class, or {@code null} if not in any scanned jar. */
    File jarOf(String name) {
        return jarByDottedName.get(normalize(name));
    }

    /**
     * True iff the class is effectively {@code @Public} — directly or via enclosing-class
     * inheritance, regardless of source-level access. Walks the enclosing chain just like
     * {@link #isDeprecated}; explicit {@code @InterfaceAudience.Private} on a nested class
     * overrides an inherited {@code @Public}.
     */
    boolean isEffectivelyPublic(String name) {
        ClassFacts hit = findInChain(name, f -> f.isPrivate() || f.isPublic());
        return hit != null && hit.isPublic();
    }

    /**
     * True iff the class — or any enclosing class — carries {@code @Deprecated}. Deprecation
     * propagates through nesting so a nested class of a {@code @Deprecated} outer is itself
     * out of scope on both validation sides (mirrors the {@code @Public} inheritance model).
     */
    boolean isDeprecated(String name) {
        return findInChain(name, ClassFacts::isDeprecated) != null;
    }

    /**
     * Walk the enclosing chain and return the first {@link ClassFacts} for which {@code stopAt}
     * is true, or {@code null} if nothing matches before we reach the top-level class.
     *
     * <p>When the current level resolves to {@link ClassFacts}, stepping uses
     * {@link ClassFacts#enclosingName} (works for both binary and dotted-form input). When it
     * doesn't — e.g. an anonymous intermediate like {@code Outer$1} that
     * {@link ApiSurfaceScanner#isSyntheticOrAnonymous} dropped — we fall back to lexical
     * {@code $}-stripping so the chain continues past the gap. That fallback matches the
     * scanner's own {@code resolveEffectiveAudience} so the two walks agree on what counts as
     * an effective audience.
     */
    private ClassFacts findInChain(String name, Predicate<ClassFacts> stopAt) {
        String current = name;
        while (current != null) {
            ClassFacts facts = factsOf(current);
            if (facts != null) {
                if (stopAt.test(facts)) return facts;
                current = facts.enclosingName();
            } else {
                String parent = ClassFacts.parentBinaryName(current);
                if (parent == null) return null;
                current = parent;
            }
        }
        return null;
    }

    /**
     * Treat {@code $} purely as a Java-style nesting separator. This is correct for Java
     * source-level nested classes and for the {@code @InterfaceAudience.Public} surface (which
     * is itself defined in plain Java). It's a known oversimplification for Scala/Kotlin
     * compiled output — e.g. Scala companion-object names ({@code Foo$}) or anonymous-function
     * synthetics ({@code Foo$$anonfun$1}) — but those compiler-generated symbols are not part
     * of the Public surface, so any normalization confusion they cause stays below the
     * checker's prefix gate.
     */
    private static String normalize(String name) {
        return name.indexOf('$') < 0 ? name : name.replace('$', '.');
    }

    static Builder builder() {
        return new Builder();
    }

    /** Accumulator used by {@link ApiSurfaceScanner}; {@link #build()} freezes into an {@link ApiSurface}. */
    static final class Builder {
        private final Set<ClassFacts> effectivePublic = new HashSet<>();
        private final Set<ClassFacts> directPublic = new HashSet<>();
        private final Map<String, ClassFacts> byDottedName = new HashMap<>();
        private final Map<String, File> jarByDottedName = new HashMap<>();

        /** Record a class's facts and the jar it came from. First jar wins for duplicates. */
        Builder recordClass(ClassFacts facts, File jar) {
            byDottedName.put(facts.dottedName(), facts);
            jarByDottedName.putIfAbsent(facts.dottedName(), jar);
            return this;
        }

        /** Add a class that is effectively {@code @Public} (direct or inherited), any visibility. */
        Builder addEffectivePublic(ClassFacts facts) {
            effectivePublic.add(facts);
            return this;
        }

        Builder addDirectPublic(ClassFacts facts) {
            directPublic.add(facts);
            return this;
        }

        ApiSurface build() {
            return new ApiSurface(this);
        }
    }
}