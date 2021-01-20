package day02.transform;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class conenct {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> s1 = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> s2 = env.socketTextStream("hadoop102", 8888);
        ConnectedStreams<String, String> connect = s1.connect(s2);
        SingleOutputStreamOperator<Object> map = connect.map(new CoMapFunction<String, String, Object>() {
            @Override
            public Object map1(String s) throws Exception {
                return s.length();
            }

            @Override
            public Object map2(String s) throws Exception {
                return s;
            }
        });

        map.print();
        env.execute();
    }
}
