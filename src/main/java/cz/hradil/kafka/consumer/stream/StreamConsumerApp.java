package cz.hradil.kafka.consumer.stream;


import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.util.Properties;

public class StreamConsumerApp {

    private KafkaStreams streams;

    public static void main(String[] args) {
        StreamConsumerApp app = new StreamConsumerApp();
        app.runExample();
    }

    public void runExample() {
        Topology topology = new Topology();

        topology.addSource(Topology.AutoOffsetReset.EARLIEST, "source1", "fh-topic1");
        topology.addProcessor("validator", validatorSupplier(), "source1");
        topology.addProcessor("core", coreSupplier(), "validator");
        topology.addProcessor("outbound", outboundSupplier(), "core");
        topology.addSink("sink1", "fh-topic2", "outbound");

        streams = new KafkaStreams(topology, createKafkaProperties());
        streams.start();

        System.out.println("Streams started...");
    }

    private Properties createKafkaProperties() {
        Properties properties = new Properties();
        properties.put("application.id", "consumer-1");
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        return properties;
    }

    private ProcessorSupplier validatorSupplier() {
        return new ProcessorSupplier() {
            public Processor get() {
                return new ValidatorProcessor();
            }
        };
    }

    private ProcessorSupplier coreSupplier() {
        return new ProcessorSupplier() {
            public Processor get() {
                return new CoreProcessor();
            }
        };
    }

    private ProcessorSupplier outboundSupplier() {
        return new ProcessorSupplier() {
            public Processor get() {
                return new OutboundProcessor();
            }
        };
    }

}
