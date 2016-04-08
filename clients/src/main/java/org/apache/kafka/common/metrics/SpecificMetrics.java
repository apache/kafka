package org.apache.kafka.common.metrics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.utils.Time;

/**
 * @author jcheng
 *
 *         A metrics factory that lets you generate metrics based on a template.
 *         The template is used to generate documentation.
 */
public class SpecificMetrics extends Metrics {

    public SpecificMetrics(MetricConfig defaultConfig, List<MetricsReporter> reporters, Time time,
            boolean enableExpiration) {
        super(defaultConfig, reporters, time, enableExpiration);

    }

    public MetricName metricInstance(MetricNameTemplate template, String... keyValue) {
        return metricInstance(template, getTags(keyValue));
    }

    private static Map<String, String> getTags(String... keyValue) {
        if ((keyValue.length % 2) != 0)
            throw new IllegalArgumentException("keyValue needs to be specified in pairs");
        Map<String, String> tags = new HashMap<String, String>();

        for (int i = 0; i < keyValue.length; i += 2)
            tags.put(keyValue[i], keyValue[i + 1]);
        return tags;
    }

    public MetricName metricInstance(MetricNameTemplate template, Map<String, String> tags) {
        return super.metricName(template.name(), template.group(), template.description(), tags);
    }

    /**
     * Deprecated this simply so that I could drop this class in as a
     * replacement for Metrics, but have my IDE tell me all the references that
     * are instantiating metrics directly, rather than using references to my
     * instances.
     */
    @Deprecated
    @Override
    public MetricName metricName(String name, String group, String description, Map<String, String> tags) {
        // TODO Auto-generated method stub
        return super.metricName(name, group, description, tags);
    }

    @Deprecated
    @Override
    public MetricName metricName(String name, String group, String description) {
        // TODO Auto-generated method stub
        return super.metricName(name, group, description);
    }

    @Deprecated
    @Override
    public MetricName metricName(String name, String group) {
        // TODO Auto-generated method stub
        return super.metricName(name, group);
    }

    @Deprecated
    @Override
    public MetricName metricName(String name, String group, String description, String... keyValue) {
        // TODO Auto-generated method stub
        return super.metricName(name, group, description, keyValue);
    }

    @Deprecated
    @Override
    public MetricName metricName(String name, String group, Map<String, String> tags) {
        // TODO Auto-generated method stub
        return super.metricName(name, group, tags);
    }

    public static String toHtmlTable(String domain, MetricNameTemplate[] allMetrics) {
        // TODO Auto-generated method stub
        Metrics metrics = new Metrics();
        Map<String, Map<String, String>> beansAndAttributes = new HashMap<String, Map<String, String>>();
        for (MetricNameTemplate template : allMetrics) {
            Map<String, String> tags = new HashMap<String, String>();
            for (String s : template.tags()) {
                tags.put(s, "{" + s + "}");
            }

            MetricName metricName = metrics.metricName(template.name(), template.group(), template.description(), tags);
            String mBeanName = JmxReporter.getMBeanName(domain, metricName);
            if (!beansAndAttributes.containsKey(mBeanName)) {
                beansAndAttributes.put(mBeanName, new HashMap<String, String>());
            }
            Map<String, String> attrAndDesc = beansAndAttributes.get(mBeanName);
            attrAndDesc.put(template.name(), template.description());
        }
        StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\"><tbody>\n");
        b.append("<tr>\n");
        b.append("<th>Mbean name</th>\n");
        b.append("<th>Attribute name</th>\n");
        b.append("<th>Description</th>\n");
        b.append("</tr>\n");

        for (Entry<String, Map<String, String>> e : beansAndAttributes.entrySet()) {
            b.append("<tr>\n");
            b.append("<td colspan=3>");
            b.append(e.getKey());
            b.append("</td>");
            b.append("</tr>\n");

            for (Entry<String, String> e2 : e.getValue().entrySet()) {
                b.append("<tr>\n");
                b.append("<td></td>");
                b.append("<td>");
                b.append(e2.getKey());
                b.append("</td>");
                b.append("<td>");
                b.append(e2.getValue());
                b.append("</td>");
                b.append("</tr>\n");
            }

        }
        b.append("</tbody></table>");
        metrics.close();
        return b.toString();

    }

}
