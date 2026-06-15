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
package org.apache.kafka.publicapi;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

    /** Externally-visible effectively-Public classes — drives the cascade iteration. */
    private final Set<ClassFacts> effectivePublic;
    /** Classes carrying a direct {@code @InterfaceAudience.Public} — drives the MISSING_JAVADOC iteration. */
    private final Set<ClassFacts> directPublic;
    /** Membership set used by {@link #isEffectivelyPublic} — includes inherited Public on private/package nested classes. */
    private final Set<String> effectivePublicDottedNames;
    private final Map<String, ClassFacts> byDottedName;
    private final Map<String, File> jarByDottedName;

    private ApiSurface(Builder b) {
        this.effectivePublic = Set.copyOf(b.effectivePublic);
        this.directPublic = Set.copyOf(b.directPublic);
        this.effectivePublicDottedNames = Set.copyOf(b.effectivePublicDottedNames);
        this.byDottedName = Map.copyOf(b.byDottedName);
        this.jarByDottedName = Map.copyOf(b.jarByDottedName);
    }

    /** Externally-visible classes that are effectively {@code @Public}. Cascade iterates this. */
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
     * inheritance, regardless of source-level access. Cascade reference checks use this to
     * recognise nested types whose outer carries the annotation.
     */
    boolean isEffectivelyPublic(String name) {
        return effectivePublicDottedNames.contains(normalize(name));
    }

    /**
     * True iff the class — or any enclosing class — carries {@code @Deprecated}. Deprecation
     * propagates through nesting so a nested class of a {@code @Deprecated} outer is itself
     * out of scope on both validation sides (mirrors the {@code @Public} inheritance model).
     */
    boolean isDeprecated(String name) {
        ClassFacts current = factsOf(name);
        while (current != null) {
            if (current.isDeprecated()) return true;
            String enclosing = current.enclosingName();
            if (enclosing == null) return false;
            current = factsOf(enclosing);
        }
        return false;
    }

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
        private final Set<String> effectivePublicDottedNames = new HashSet<>();
        private final Map<String, ClassFacts> byDottedName = new HashMap<>();
        private final Map<String, File> jarByDottedName = new HashMap<>();

        /** Record a class's facts and the jar it came from. First jar wins for duplicates. */
        Builder recordClass(ClassFacts facts, File jar) {
            byDottedName.put(facts.dottedName(), facts);
            jarByDottedName.putIfAbsent(facts.dottedName(), jar);
            return this;
        }

        /**
         * Membership marker: the class is effectively {@code @Public} (direct or inherited).
         * Feeds the set behind {@link #isEffectivelyPublic(String)}. Independent of cascade
         * iteration, which is governed by {@link #addEffectivePublic(ClassFacts)}.
         */
        Builder markEffectivelyPublic(ClassFacts facts) {
            effectivePublicDottedNames.add(facts.dottedName());
            return this;
        }

        /** Add a class to the cascade iteration set (externally-visible effectively-Public classes). */
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