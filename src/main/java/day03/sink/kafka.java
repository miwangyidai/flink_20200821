package day03.sink;

import com.alibaba.fastjson.JSON;
import com.sun.org.apache.bcel.internal.generic.RETURN;
import com.sun.org.apache.bcel.internal.generic.VariableLengthInstruction;
import day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.BootstrapConfig;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> input = env.readTextFile("fdsfsd");

        SingleOutputStreamOperator<WaterSensor> fDS = input.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String s, Collector<WaterSensor> collector) throws Exception {
                String[] split = s.split(",");
                collector.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }
        });

        //将javabean转化为jsonstring
        SingleOutputStreamOperator<String> mapDS = fDS.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor waterSensor) throws Exception {
                return JSON.toJSONString(waterSensor);
            }
        });

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102");

        //将数据写入到kafka
        mapDS.addSink(new FlinkKafkaProducer<String>("test",new SimpleStringSchema(),properties));

        env.execute();
    }
}
