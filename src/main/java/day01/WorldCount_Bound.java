package day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WorldCount_Bound {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> input = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = input.flatMap(new Myflatmap());
        KeyedStream<Tuple2<String, Integer>, String> KDs = wordToOneDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        });

        KDs.print("dddd");

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = KDs.sum(1);

        sum.print();

        env.execute("sdsd");


    }
    
    public static class Myflatmap implements FlatMapFunction<String, Tuple2<String,Integer>>{
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] s1 = s.split(" ");
            for (String s2 : s1) {
                collector.collect(new Tuple2<String, Integer>(s2,1));
            }
        }
    }
}
