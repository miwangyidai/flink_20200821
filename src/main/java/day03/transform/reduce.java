package day03.transform;

import day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> input = env.socketTextStream("hadoop102",6666);
        SingleOutputStreamOperator<WaterSensor> fDS = input.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String s, Collector<WaterSensor> collector) throws Exception {
                String[] split = s.split(",");
                    collector.collect(new WaterSensor(split[0], 
                            Long.parseLong(split[1]), 
                            Integer.parseInt(split[2])));

            }
        });

        KeyedStream<WaterSensor, String> sss = fDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor waterSensor) throws Exception {
                return waterSensor.getId();
            }
        });


        SingleOutputStreamOperator<WaterSensor> reduce = sss.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor waterSensor, WaterSensor t1) throws Exception {
                return new WaterSensor(
                        waterSensor.getId(),
                        waterSensor.getTs(),
                        Math.max(waterSensor.getVc(), t1.getVc()));
            }
        });

        reduce.print();
        env.execute();
    }
}
