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

    VersionConditional alwaysEmitBlockScope(boolean alwaysEmitBlockScope) {
        this.alwaysEmitBlockScope = alwaysEmitBlockScope;
        return this;
    }

    VersionConditional allowMembershipCheckAlwaysFalse(boolean allowMembershipCheckAlwaysFalse) {
        this.allowMembershipCheckAlwaysFalse = allowMembershipCheckAlwaysFalse;
        return this;
    }

    private void generateIfClause() {
        ifMember.generate(possibleVersions.intersect(containingVersions));
    }

    private void generateIfNotClause() {
        ifMember.generate(possibleVersions.trim(containingVersions));
    }

    private void generateFullRangeCheck(CodeBuffer buffer) {
        if (ifMember != null) {
            buffer.printf("if ((version >= %d) && (version <= %d)) {%n",
                    containingVersions.lowest(), containingVersions.highest());
            buffer.incrementIndent();
            generateIfClause();
            buffer.decrementIndent();
            if (ifNotMember != null) {
                buffer.printf("} else {%n");
                buffer.incrementIndent();
                generateIfNotClause();
                buffer.decrementIndent();
                buffer.printf("}%n");
            } else {
                buffer.printf("}%n");
            }
        } else if (ifNotMember != null) {
            buffer.printf("if ((version < %d) || (version > %d)) {%n",
                    containingVersions.lowest(), containingVersions.highest());
            buffer.incrementIndent();
            generateIfNotClause();
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
    }

    private void generateLowerRangeCheck(CodeBuffer buffer) {
        if (ifMember != null) {
            buffer.printf("if (version >= %d) {%n", containingVersions.lowest());
            buffer.incrementIndent();
            generateIfClause();
            buffer.decrementIndent();
            if (ifNotMember != null) {
                buffer.printf("} else {%n");
                buffer.incrementIndent();
                generateIfNotClause();
                buffer.decrementIndent();
                buffer.printf("}%n");
            } else {
                buffer.printf("}%n");
            }
        } else if (ifNotMember != null) {
            buffer.printf("if (version < %d) {%n", containingVersions.lowest());
            buffer.incrementIndent();
            generateIfNotClause();
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
    }

    private void generateUpperRangeCheck(CodeBuffer buffer) {
        if (ifMember != null) {
            buffer.printf("if (version <= %d) {%n", containingVersions.highest());
            buffer.incrementIndent();
            generateIfClause();
            buffer.decrementIndent();
            if (ifNotMember != null) {
                buffer.printf("} else {%n");
                buffer.incrementIndent();
                generateIfNotClause();
                buffer.decrementIndent();
                buffer.printf("}%n");
            } else {
                buffer.printf("}%n");
            }
        } else if (ifNotMember != null) {
            buffer.printf("if (version > %d) {%n", containingVersions.highest());
            buffer.incrementIndent();
            generateIfNotClause();
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
    }

    private void generateAlwaysTrueCheck(CodeBuffer buffer) {
        if (ifMember != null) {
            if (alwaysEmitBlockScope) {
                buffer.printf("{%n");
                buffer.incrementIndent();
            }
            generateIfClause();
            if (alwaysEmitBlockScope) {
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
        }
    }

    private void generateAlwaysFalseCheck(CodeBuffer buffer) {
        if (!allowMembershipCheckAlwaysFalse) {
            throw new RuntimeException("Version ranges " + containingVersions +
                " and " + possibleVersions + " have no versions in common.");
        }
        if (ifNotMember != null) {
            if (alwaysEmitBlockScope) {
                buffer.printf("{%n");
                buffer.incrementIndent();
            }
            generateIfNotClause();
            if (alwaysEmitBlockScope) {
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
        }
    }

    void generate(CodeBuffer buffer) {
        if (possibleVersions.lowest() < containingVersions.lowest()) {
            if (possibleVersions.highest() > containingVersions.highest()) {
                generateFullRangeCheck(buffer);
            } else {
                generateLowerRangeCheck(buffer);
            }
        } else if (possibleVersions.highest() >= containingVersions.lowest() &&
                    (possibleVersions.lowest() <= containingVersions.highest())) {
            if (possibleVersions.highest() > containingVersions.highest()) {
                generateUpperRangeCheck(buffer);
            } else {
                generateAlwaysTrueCheck(buffer);
            }
        } else {
            generateAlwaysFalseCheck(buffer);
        }
    }
}
