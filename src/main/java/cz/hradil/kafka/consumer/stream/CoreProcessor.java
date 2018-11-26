package cz.hradil.kafka.consumer.stream;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Map;

public class CoreProcessor extends AbstractProcessor<String, Map<String, String>> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    public void process(String key, Map<String, String> value) {
        System.out.println("CoreProcessor... value=" + value);
        value.put("core.item1", "item1");
        context.forward(key,value);
    }

}
