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

package org.apache.kafka.message;

/**
 * Creates an if statement based on whether or not the current version
 * falls within a given range.
 */
public final class VersionConditional {
    /**
     * Create a version conditional.
     *
     * @param containingVersions    The versions for which the conditional is true.
     * @param possibleVersions      The range of possible versions.
     * @return                      The version conditional.
     */
    static VersionConditional forVersions(Versions containingVersions,
                                          Versions possibleVersions) {
        return new VersionConditional(containingVersions, possibleVersions);
    }

    private final Versions containingVersions;
    private final Versions possibleVersions;
    private ClauseGenerator ifMember = null;
    private ClauseGenerator ifNotMember = null;
    private boolean alwaysEmitBlockScope = false;
    private boolean allowMembershipCheckAlwaysFalse = true;

    private VersionConditional(Versions containingVersions, Versions possibleVersions) {
        this.containingVersions = containingVersions;
        this.possibleVersions = possibleVersions;
    }

    VersionConditional ifMember(ClauseGenerator ifMember) {
        this.ifMember = ifMember;
        return this;
    }

    VersionConditional ifNotMember(ClauseGenerator ifNotMember) {
        this.ifNotMember = ifNotMember;
        return this;
    }

    /**
     * If this is set, we will always create a new block scope, even if there
     * are no 'if' statements.  This is useful for cases where we want to
     * declare variables in the clauses without worrying if they conflict with
     * other variables of the same name.
     */
    VersionConditional alwaysEmitBlockScope(boolean alwaysEmitBlockScope) {
        this.alwaysEmitBlockScope = alwaysEmitBlockScope;
        return this;
    }

    /**
     * If this is set, VersionConditional#generate will throw an exception if
     * the 'ifMember' clause is never used.  This is useful as a sanity check
     * in some cases where it doesn't make sense for the condition to always be
     * false.  For example, when generating a Message#write function, 
     * we might check that the version we're writing is supported.  It wouldn't
     * make sense for this check to always be false, since that would mean that
     * no versions at all were supported.
     */
    VersionConditional allowMembershipCheckAlwaysFalse(boolean allowMembershipCheckAlwaysFalse) {
        this.allowMembershipCheckAlwaysFalse = allowMembershipCheckAlwaysFalse;
        return this;
    }

    private void generateFullRangeCheck(Versions ifVersions,
                                        Versions ifNotVersions,
                                        CodeBuffer buffer) {
        if (ifMember != null) {
            buffer.printf("if ((_version >= %d) && (_version <= %d)) {%n",
                    containingVersions.lowest(), containingVersions.highest());
            buffer.incrementIndent();
            ifMember.generate(ifVersions);
            buffer.decrementIndent();
            if (ifNotMember != null) {
                buffer.printf("} else {%n");
                buffer.incrementIndent();
                ifNotMember.generate(ifNotVersions);
                buffer.decrementIndent();
            }
            buffer.printf("}%n");
        } else if (ifNotMember != null) {
            buffer.printf("if ((_version < %d) || (_version > %d)) {%n",
                    containingVersions.lowest(), containingVersions.highest());
            buffer.incrementIndent();
            ifNotMember.generate(ifNotVersions);
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
    }

    private void generateLowerRangeCheck(Versions ifVersions,
                                         Versions ifNotVersions,
                                         CodeBuffer buffer) {
        if (ifMember != null) {
            buffer.printf("if (_version >= %d) {%n", containingVersions.lowest());
            buffer.incrementIndent();
            ifMember.generate(ifVersions);
            buffer.decrementIndent();
            if (ifNotMember != null) {
                buffer.printf("} else {%n");
                buffer.incrementIndent();
                ifNotMember.generate(ifNotVersions);
                buffer.decrementIndent();
            }
            buffer.printf("}%n");
        } else if (ifNotMember != null) {
            buffer.printf("if (_version < %d) {%n", containingVersions.lowest());
            buffer.incrementIndent();
            ifNotMember.generate(ifNotVersions);
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
    }

    private void generateUpperRangeCheck(Versions ifVersions,
                                         Versions ifNotVersions,
                                         CodeBuffer buffer) {
        if (ifMember != null) {
            buffer.printf("if (_version <= %d) {%n", containingVersions.highest());
            buffer.incrementIndent();
            ifMember.generate(ifVersions);
            buffer.decrementIndent();
            if (ifNotMember != null) {
                buffer.printf("} else {%n");
                buffer.incrementIndent();
                ifNotMember.generate(ifNotVersions);
                buffer.decrementIndent();
            }
            buffer.printf("}%n");
        } else if (ifNotMember != null) {
            buffer.printf("if (_version > %d) {%n", containingVersions.highest());
            buffer.incrementIndent();
            ifNotMember.generate(ifNotVersions);
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
    }

    private void generateAlwaysTrueCheck(Versions ifVersions, CodeBuffer buffer) {
        if (ifMember != null) {
            if (alwaysEmitBlockScope) {
                buffer.printf("{%n");
                buffer.incrementIndent();
            }
            ifMember.generate(ifVersions);
            if (alwaysEmitBlockScope) {
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
        }
    }

    private void generateAlwaysFalseCheck(Versions ifNotVersions, CodeBuffer buffer) {
        if (!allowMembershipCheckAlwaysFalse) {
            throw new RuntimeException("Version ranges " + containingVersions +
                " and " + possibleVersions + " have no versions in common.");
        }
        if (ifNotMember != null) {
            if (alwaysEmitBlockScope) {
                buffer.printf("{%n");
                buffer.incrementIndent();
            }
            ifNotMember.generate(ifNotVersions);
            if (alwaysEmitBlockScope) {
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
        }
    }

    void generate(CodeBuffer buffer) {
        Versions ifVersions = possibleVersions.intersect(containingVersions);
        Versions ifNotVersions = possibleVersions.subtract(containingVersions);
        // In the case where ifNotVersions would be two ranges rather than one,
        // we just pass in the original possibleVersions instead.
        // This is slightly less optimal, but allows us to avoid dealing with
        // multiple ranges.
        if (ifNotVersions == null) {
            ifNotVersions = possibleVersions;
        }

        if (possibleVersions.lowest() < containingVersions.lowest()) {
            if (possibleVersions.highest() > containingVersions.highest()) {
                generateFullRangeCheck(ifVersions, ifNotVersions, buffer);
            } else if (possibleVersions.highest() >= containingVersions.lowest()) {
                generateLowerRangeCheck(ifVersions, ifNotVersions, buffer);
            } else {
                generateAlwaysFalseCheck(ifNotVersions, buffer);
            }
        } else if (possibleVersions.highest() >= containingVersions.lowest() &&
                    (possibleVersions.lowest() <= containingVersions.highest())) {
            if (possibleVersions.highest() > containingVersions.highest()) {
                generateUpperRangeCheck(ifVersions, ifNotVersions, buffer);
            } else {
                generateAlwaysTrueCheck(ifVersions, buffer);
            }
        } else {
            generateAlwaysFalseCheck(ifNotVersions, buffer);
        }
    }
}
