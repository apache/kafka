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
package org.apache.kafka.common.message;

import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.ObjectStream;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.AndTreeFilter;
import org.eclipse.jgit.treewalk.filter.PathFilter;
import org.eclipse.jgit.treewalk.filter.PathSuffixFilter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Git {

    private static final String REFS_TAGS = "refs/tags/";
    private static final Pattern TAG_PATTERN = Pattern.compile(REFS_TAGS + "([0-9]+(\\.[0-9]+)+)");
    private static Set<Ref> tags;
    public static final Repository REPO;
    static {
        try {
            REPO = new FileRepositoryBuilder()
                    .setGitDir(findGitDir())
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Finds the .git directory by searching from the cwd upwards
     */
    public static File findGitDir() {
        File start = new File(".").getAbsoluteFile();
        File f = start;
        do {
            File gitDir = new File(f, ".git");
            if (gitDir.exists() && gitDir.isDirectory()) {
                return gitDir;
            }
            f = f.getParentFile();
        } while (f != null);
        throw new RuntimeException("Cannot find git directory searching up from " + start);
    }

    public interface Parser<X> {
        X parse(KafkaVersion kafkaVersion, InputStream input) throws IOException;
    }

    /**
     * Returns a Stream of the git tags of Kafka releases, in increasing release order.
     */
    public static Stream<Ref> kafkaReleaseTags() throws IOException {
        return REPO.getRefDatabase().getRefsByPrefix(Constants.R_TAGS).stream()
                .filter(ref -> TAG_PATTERN.matcher(ref.getName()).matches())
                .collect(Collectors.toCollection(()-> new TreeSet<>(new VersionComparator())))
                .stream();
    }

    /**
     * Returns a Stream of git HEAD.
     */
    public static Stream<Ref> head() throws IOException {
        return REPO.getRefDatabase().getRefsByPrefix(Constants.HEAD)
                .stream();
    }

    /**
     * Comparator for version numbers
     */
    private static class VersionComparator implements Comparator<Ref> {
        @Override
        public int compare(Ref o1, Ref o2) {
            if (o1.getName().equals(o2.getName())) {
                return 0;
            }
            List<Integer> parts1 = parseVersion(o1);
            List<Integer> parts2 = parseVersion(o2);
            for (int i = 0; i <= Math.min(parts1.size(), parts2.size()) - 1; i++) {
                int cmp = parts1.get(i).compareTo(parts2.get(i));
                if (cmp != 0) {
                    return cmp;
                }
            }
            return Integer.compare(parts1.size(), parts2.size());
        }

        private static List<Integer> parseVersion(Ref tag) {
            return version(tag).parts();
        }
    }

    public static KafkaVersion version(Ref tag) {
        return KafkaVersion.parse(tag.getName().startsWith(REFS_TAGS) ? tag.getName().substring(REFS_TAGS.length()) : tag.getName());
    }

    public static <X> Stream<VersionAndSpec<X>> parseFiles(Ref ref, String path, String pathSuffix, Parser<X> parser) {
        List<ObjectId> objectIds = new ArrayList<>();
        try {
            RevWalk walk = new RevWalk(Git.REPO);
            RevTree tree = walk.parseTree(ref.getObjectId());
            TreeWalk treeWalk = new TreeWalk(Git.REPO);
            treeWalk.addTree(tree);
            treeWalk.setRecursive(true);
            treeWalk.setFilter(AndTreeFilter.create(
                    PathFilter.create(path),
                    PathSuffixFilter.create(pathSuffix)));
            while (treeWalk.next()) {
                ObjectId objectId = treeWalk.getObjectId(0);
                objectIds.add(objectId);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        KafkaVersion kafkaVersion = version(ref);
        return objectIds.stream().map(objectId -> parse(parser, kafkaVersion, objectId));
    }

    private static <X> VersionAndSpec<X> parse(Parser<X> parser, KafkaVersion kafkaVersion, ObjectId objectId) {
        try {
            ObjectLoader loader = Git.REPO.open(objectId);
            try (ObjectStream input = loader.openStream()) {
                return new VersionAndSpec<>(kafkaVersion, parser.parse(kafkaVersion, input));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class VersionAndSpec<X> {
        private final KafkaVersion kafkaVersion;
        private final X model;

        VersionAndSpec(KafkaVersion kafkaVersion, X model) {
            this.kafkaVersion = kafkaVersion;
            this.model = model;
        }

        public KafkaVersion kafkaVersion() {
            return kafkaVersion;
        }

        public X model() {
            return model;
        }

        public String toString() {
            return "(" + kafkaVersion + "," + model + ")";
        }
    }
}
