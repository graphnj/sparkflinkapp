package com.huawei.zjh;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * Created by zhujinhua on 2017/10/29.
 */
public class KafkaMessageStreaming {



    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // 非常关键，一定要设置启动检查点！！
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "flink-group");

        props.load(new FileInputStream("application.properties"));

        String input_topic = props.getProperty("input_topic");
        String output_file = props.getProperty("output_file");
        Long   timewindows = Long.parseLong(props.getProperty("time_windows"));
        String function = props.getProperty("function");

        FlinkKafkaConsumer010<String> consumer =
                new FlinkKafkaConsumer010<>(input_topic, new SimpleStringSchema(), props);
        consumer.assignTimestampsAndWatermarks(new MessageWaterEmitter());

        /*DataStream<Tuple2<String, Long>> keyedStream = env
                .addSource(consumer)
                .flatMap(new MessageSplitter())
                .keyBy(0)
                .timeWindow(Time.seconds(10));*/

        WindowedStream<Tuple2<String, Long>, Tuple, TimeWindow> windowedStream = env
                .addSource(consumer)
                .flatMap(new MessageSplitter())
                .keyBy(0)
                .timeWindow(Time.seconds(timewindows));


        WindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple, TimeWindow> expectWindowFunction = null;

        if (function.equals("avg")) {
            expectWindowFunction = new windows_avg();
        }
        else if (function.equals("max")){
            expectWindowFunction = new windows_max();
        }
        else if (function.equals("min")){
            expectWindowFunction = new windows_min();
        }

        DataStream<Tuple2<String, Long>> keyedStream  = windowedStream.apply(expectWindowFunction);

        keyedStream.writeAsText(output_file, FileSystem.WriteMode.OVERWRITE);
        env.execute("Flink-Kafka demo");
    }
}

class windows_avg implements WindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple, TimeWindow> {
    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Tuple2<String, Long>> out) throws Exception {
        long sum = 0L;
        int count = 0;
        for (Tuple2<String, Long> record: input) {
            sum += record.f1;
            count++;
        }
        Tuple2<String, Long> result = input.iterator().next();
        result.f1 = sum / count;
        out.collect(result);
    }
}

class windows_max implements WindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple, TimeWindow> {
    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Tuple2<String, Long>> out) throws Exception {
        long maxval = Long.MIN_VALUE;

        long sum = 0L;
        int count = 0;
        for (Tuple2<String, Long> record: input) {
            if(record.f1 > maxval)
                maxval = record.f1;
        }
        Tuple2<String, Long> result = input.iterator().next();
        result.f1 = maxval;
        out.collect(result);
    }
}

class windows_min implements WindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple, TimeWindow> {
    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Tuple2<String, Long>> out) throws Exception {
        long minval = Long.MAX_VALUE;

        long sum = 0L;
        int count = 0;
        for (Tuple2<String, Long> record: input) {
            if(record.f1 < minval)
                minval = record.f1;
        }
        Tuple2<String, Long> result = input.iterator().next();
        result.f1 = minval;
        out.collect(result);
    }
}