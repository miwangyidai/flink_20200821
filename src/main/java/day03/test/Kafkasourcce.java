package day03.test;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Kafkasourcce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadooop:9092");
        properties.setProperty("group.id", "test");
        env.addSource(new FlinkKafkaConsumer<String>("test",new SimpleStringSchema(),properties)).print();
        env.execute();
    }
}
