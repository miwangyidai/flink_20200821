package day03.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class test06 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final HashSet<String> set = new HashSet<>();
        //读取端口数据
        DataStreamSource<String> aliyunDS = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<String> result = aliyunDS.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] words = line.split(",");
                for (String word : words) {
                    if (set.contains(word)){

                    }else {
                        set.add(word);
                    }

                    boolean add = set.add(word);
                    if (add) {
                        collector.collect(word);
                    }
                }
            }
        });

        result.print();
        env.execute();

    }
}
