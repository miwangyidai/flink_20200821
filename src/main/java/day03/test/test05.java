package day03.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Iterator;

public class test05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<String> input = env.socketTextStream("hadoop102", 9999)
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        String[] split = s.split(",");
                        for (String s1 : split) {
                            collector.collect(s1);
                        }
                    }
                });

        HashSet<String> strings = new HashSet<>();
         strings.add(input.toString());



        Iterator<String> iterator = strings.iterator();
        while (iterator.hasNext()){
            System.out.println("dsd");
            System.out.println(iterator.next());
        }
        env.execute();


    }
}
