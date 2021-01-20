package day03.sink;

import com.alibaba.fastjson.JSON;
import day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class Es {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> input = env.socketTextStream("hadoop102",7777);

        SingleOutputStreamOperator<WaterSensor> fDS = input.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String s, Collector<WaterSensor> collector) throws Exception {
                String[] split = s.split(",");
                collector.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }
        });


        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102",9200));
        ElasticsearchSink.Builder<WaterSensor> waterSensorBuilder = new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunction<WaterSensor>() {
            @Override
            public void process(WaterSensor waterSensor, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                HashMap<String, String> map = new HashMap<>();
                map.put("ts", waterSensor.getTs().toString());
                map.put("vc", waterSensor.getVc().toString());


                //创建index请求
                IndexRequest doc = Requests.indexRequest()
                        .index("my-index")
                        .type("_doc")
                        .source(map);
                requestIndexer.add(doc);
            }
        });
        waterSensorBuilder.setBulkFlushMaxActions(1);
        ElasticsearchSink<WaterSensor> build = waterSensorBuilder.build();
        fDS.addSink(build);
        env.execute();
    }
}
