package day03.sink;

import day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class pro {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> input = env.readTextFile("D:\\pro\\flink_20200821\\test\\test03.txt");

        SingleOutputStreamOperator<WaterSensor> fDS = input.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String s, Collector<WaterSensor> collector) throws Exception {
                String[] split = s.split(",");
                collector.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> process = fDS.keyBy("id").process(new KeyedProcessFunction<Tuple, WaterSensor, Tuple2<String,Integer>>() {private ValueState<Integer> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("dssdd", Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                Integer value1 = state.value();
                if (value1 == null) {
                    value1 = 0;
                }
                Integer vc = value.getVc();
                int i = value1 + vc;
                out.collect(new Tuple2<String,Integer>(value.getId(),i));
                state.update(i);
            }
        });


        process.print();
        env.execute();
    }
}
