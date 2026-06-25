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

import org.objectweb.asm.Opcodes;

import java.util.EnumSet;

/**
 * Everything the checker needs to know about a single class, read once via ASM and cached.
 * Carries:
 * <ul>
 *   <li>The class's two names — binary ({@code org.apache.kafka.X$Y}) for jar-entry lookup,
 *       dotted ({@code org.apache.kafka.X.Y}) for javadoc HTML comparison.</li>
 *   <li>The set of class-level bytecode annotations the checker recognises (see {@link Flag}).</li>
 *   <li>The class's source-level access flag — for top-level classes the compiler writes the
 *       true source access on the class header; for nested classes the header is always
 *       {@code ACC_PUBLIC} regardless of source, but the real access lives in the
 *       {@code InnerClasses} attribute entry for the class itself. The scanner reads both, with
 *       the inner-class entry winning when present.</li>
 * </ul>
 *
 * <p>Reading from bytecode rather than loading the class sidesteps {@code LinkageError} /
 * {@code NoClassDefFoundError} from broken transitive deps (gRPC stubs, telemetry shims, etc.) —
 * annotation descriptors live in the class file's constant pool, no linking required.
 *
 * <p>Instances are immutable; construct via {@link #builder(String)} so the ASM visitor that
 * populates each field has an explicit mutable target instead of poking the result type.
 * Equality is by {@code binaryName} so instances can be deduplicated in sets.
 */
final class ClassFacts {

    /** Class-level bytecode annotation markers the checker cares about. */
    enum Flag {
        PUBLIC_API,
        PRIVATE_API,
        DEPRECATED
    }

    private final String binaryName;
    private final String dottedName;
    private final EnumSet<Flag> flags;
    private final int sourceAccess;

    private ClassFacts(Builder b) {
        this.binaryName = b.binaryName;
        this.dottedName = b.binaryName.replace('$', '.');
        this.flags = b.flags.isEmpty() ? EnumSet.noneOf(Flag.class) : EnumSet.copyOf(b.flags);
        this.sourceAccess = b.sourceAccess;
    }

    String binaryName() {
        return binaryName;
    }

    String dottedName() {
        return dottedName;
    }

    /** Carries a direct {@code @InterfaceAudience.Public}. */
    boolean isPublic() {
        return flags.contains(Flag.PUBLIC_API);
    }

    /** Carries a direct {@code @InterfaceAudience.Private} — overrides inherited Public on a nested class. */
    boolean isPrivate() {
        return flags.contains(Flag.PRIVATE_API);
    }

    /** Carries {@code @Deprecated} at the class level. */
    boolean isDeprecated() {
        return flags.contains(Flag.DEPRECATED);
    }

    /** True iff source-level access is {@code public} or {@code protected}. */
    boolean isExternallyVisible() {
        return (sourceAccess & (Opcodes.ACC_PUBLIC | Opcodes.ACC_PROTECTED)) != 0;
    }

    /**
     * @return dotted name of the enclosing class, or {@code null} if this is a top-level class.
     *         Lets chain walks avoid manipulating the {@code $} separator directly.
     */
    String enclosingName() {
        String parent = parentBinaryName(binaryName);
        return parent == null ? null : parent.replace('$', '.');
    }

    /**
     * @return the immediate-parent binary name of a nested class (strip the trailing
     *         {@code $segment}), or {@code null} for top-level classes. The single canonical
     *         way to step one level outward in the enclosing-class chain.
     */
    static String parentBinaryName(String binaryName) {
        int dollar = binaryName.lastIndexOf('$');
        return dollar < 0 ? null : binaryName.substring(0, dollar);
    }

    /**
     * @return the outermost compilation-unit binary name — strip everything from the first
     *         {@code $} onward. Used for self-reference detection (two binary names share the
     *         same outermost iff they're nested under the same top-level class).
     */
    static String outermostBinaryName(String binaryName) {
        int dollar = binaryName.indexOf('$');
        return dollar < 0 ? binaryName : binaryName.substring(0, dollar);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ClassFacts)) return false;
        return binaryName.equals(((ClassFacts) o).binaryName);
    }

    @Override
    public int hashCode() {
        return binaryName.hashCode();
    }

    static Builder builder(String binaryName) {
        return new Builder(binaryName);
    }

    /** Mutable accumulator used by the ASM visitor; {@link #build()} freezes into a {@link ClassFacts}. */
    static final class Builder {
        private final String binaryName;
        private final EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
        private int sourceAccess;

        private Builder(String binaryName) {
            this.binaryName = binaryName;
        }

        Builder addFlag(Flag flag) {
            flags.add(flag);
            return this;
        }

        Builder sourceAccess(int access) {
            this.sourceAccess = access;
            return this;
        }

        ClassFacts build() {
            return new ClassFacts(this);
        }
    }
}