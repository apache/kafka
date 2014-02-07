package org.apache.kafka.common.metrics;

import java.util.List;

/**
 * A compound stat is a stat where a single measurement and associated data structure feeds many metrics. This is the
 * example for a histogram which has many associated percentiles.
 */
public interface CompoundStat extends Stat {

    public List<NamedMeasurable> stats();

    public static class NamedMeasurable {

        private final String name;
        private final String description;
        private final Measurable stat;

        public NamedMeasurable(String name, String description, Measurable stat) {
            super();
            this.name = name;
            this.description = description;
            this.stat = stat;
        }

        public String name() {
            return name;
        }

        public String description() {
            return description;
        }

        public Measurable stat() {
            return stat;
        }

    }

}
