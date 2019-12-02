package kafkaStreams;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcess implements Processor<byte[],byte[]> {


    //上下文信息，管家
    private ProcessorContext context;

    /**
     * 初始化方法
     * @param context
     */
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    //用于处理数据
    @Override
    public void process(byte[] dummy, byte[] line) {
        String input = new String(line);
        if(input.contains("MOVIE_RATING_PREFIX:")){
            input = input.split("MOVIE_RATING_PREFIX:")[1].trim();
            context.forward("logProcess".getBytes(), input.getBytes());
        }
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
