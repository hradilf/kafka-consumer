package cz.hradil.kafka.consumer.stream;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.HashMap;
import java.util.Map;


public class ValidatorProcessor extends AbstractProcessor<String, byte[]> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    public void process(String key, byte[] value) {
        System.out.println("ValidatorProcessor... value=" + new String(value));

        Map<String, String> data = new HashMap<String, String>();
        data.put(key, new String(value));
        context.forward(key, data);

    }
}
