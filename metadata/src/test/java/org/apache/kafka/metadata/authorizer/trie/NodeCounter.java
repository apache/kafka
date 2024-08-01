package org.apache.kafka.metadata.authorizer.trie;

import java.util.function.Predicate;

/**
 * Simple class to count the number of populated nodes in a Trie.
 * @param <T>
 */
class NodeCounter<T> implements Predicate<Node<T>> {
    int counter;

    public NodeCounter() {
        counter = 0;
    }

    public int count() {
        int result = counter;
        counter = 0;
        return result;
    }

    @Override
    public boolean test(Node<T> node) {
        if (node.getContents() != null) {
            ++counter;
        }
        return false;
    }
}
