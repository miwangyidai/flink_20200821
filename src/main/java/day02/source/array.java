package day02.source;

import day02.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class array {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<WaterSensor> waterSensors = Arrays.asList(
                new WaterSensor("1", 21312312313L, 21),
                new WaterSensor("2", 35312312313L, 51),
                new WaterSensor("3", 35312345331L, 26)
        );


        env.fromCollection(waterSensors).print("sss");

        env.execute();


    }
}
