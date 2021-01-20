package day03.sink;

import day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisSentinelConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

public class Redis {
    public static void main(String[] args) {
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

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .build();

        DataStreamSink<WaterSensor> sss = fDS.addSink(new RedisSink<WaterSensor>(conf, new RedisMapper<WaterSensor>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "sss");
            }

            @Override
            public String getKeyFromData(WaterSensor waterSensor) {
                return waterSensor.getId();
            }

            @Override
            public String getValueFromData(WaterSensor waterSensor) {
                return waterSensor.getVc().toString();
            }
        }));
    }
}
