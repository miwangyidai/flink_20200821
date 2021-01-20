package day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WorldCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> Ds = env.readTextFile("D:\\pro\\flink_20200821\\test\\test01.txt");
        FlatMapOperator<String, String> fDs = Ds.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] s1 = s.split(" ");
                for (String s2 : s1) {
                    collector.collect(s2);
                }
            }
        });



        MapOperator<String, Tuple2<String, Integer>> mDs = fDs.map(new MapFunction<String, Tuple2<String, Integer>>() {
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });



        UnsortedGrouping<Tuple2<String, Integer>> gDS = mDs.groupBy(0);



        AggregateOperator<Tuple2<String, Integer>> sDS = gDS.sum(1);

        sDS.print();


    }
}






