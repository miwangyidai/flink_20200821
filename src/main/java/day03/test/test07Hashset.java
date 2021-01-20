package day03.test;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class test07Hashset {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final HashSet<String> set = new HashSet<>();
        //读取端口数据
        DataStreamSource<String> aliyunDS = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<String> input = aliyunDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split(",");
                for (String s1 : split) {
                    collector.collect(s1);
                }
            }
        });

        SingleOutputStreamOperator<String> filter = input.keyBy(d->d).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                boolean contains = set.contains(s);
                if (contains == true) {
                    return false;
                } else {
                    set.add(s);
                    return true;
                }
            }
        });

        filter.print("sff");
        env.execute();



    }
}
