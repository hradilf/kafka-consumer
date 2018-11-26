package cz.hradil.kafka.consumer.stream;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Map;

public class OutboundProcessor extends AbstractProcessor<String, Map<String, String>> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    public void process(String key, Map<String, String> value) {
        System.out.println("OutboundProcessor... value=" + value);

        String result = value.toString();
        context.forward(key,result.getBytes());
    }

}
