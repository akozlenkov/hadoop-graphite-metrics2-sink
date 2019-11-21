package ru.hh.hadoop.metrics2.sink;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class GraphiteSink implements MetricsSink {
    private static final Logger LOG = LoggerFactory.getLogger(GraphiteSink.class);
    private static final String SERVER_HOST_KEY = "server_host";
    private static final String SERVER_PORT_KEY = "server_port";
    private static final String METRICS_PREFIX = "metrics_prefix";
    private String metricsPrefix = null;
    private String serverHost;
    private int serverPort;


    public void putMetrics(MetricsRecord record) {
        StringBuilder lines = new StringBuilder();
        StringBuilder metricsPathPrefix = new StringBuilder();

        // Configure the hierarchical place to display the graph.
        metricsPathPrefix.append(metricsPrefix).append(".")
                .append(record.context()).append(".").append(record.name());

        for (MetricsTag tag : record.tags()) {
            if (tag.value() != null) {
                metricsPathPrefix.append(".")
                        .append(tag.name())
                        .append("=")
                        .append(tag.value());
            }
        }

        // The record timestamp is in milliseconds while Graphite expects an epoc time in seconds.
        long timestamp = record.timestamp() / 1000L;

        // Collect datapoints.
        for (AbstractMetric metric : record.metrics()) {
            lines.append(
                    metricsPathPrefix.toString() + "."
                            + metric.name().replace(' ', '.')).append(" ")
                    .append(metric.value()).append(" ").append(timestamp)
                    .append("\n");
        }

        try {
            InetAddress host = InetAddress.getByName(serverHost);
            DatagramSocket socket = new DatagramSocket (null);
            byte[] data = lines.toString().getBytes();
            DatagramPacket packet = new DatagramPacket(data, data.length, host, serverPort);
            socket.send(packet);
            socket.close();
        } catch (Exception e) {
            LOG.warn("Error sending metrics to Graphite", e);
        }
    }

    public void flush() {}

    public void init(SubsetConfiguration conf) {

        // Get Graphite host configurations.
        serverHost = conf.getString(SERVER_HOST_KEY);
        serverPort = Integer.parseInt(conf.getString(SERVER_PORT_KEY));

        // Get Graphite metrics graph prefix.
        metricsPrefix = conf.getString(METRICS_PREFIX);
        if (metricsPrefix == null) {
            metricsPrefix = "";
        }
    }
}
