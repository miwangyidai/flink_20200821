package day02.source;

import day02.bean.WaterSensor;
import org.apache.commons.math3.geometry.enclosing.EnclosingBall;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import javax.sound.sampled.Port;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Zidingyi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new Mysource("hadoop102",9999)).print();
        env.execute();
    }

    public static class Mysource implements SourceFunction<WaterSensor>{
        private String host;
        private Integer port;
        private volatile boolean isRunning = true;
        private Socket socket;

        public Mysource(String host, Integer port) {
            this.host = host;
            this.port = port;
        }

        public Mysource() {
        }

        public void run(SourceContext<WaterSensor> sourceContext) throws Exception {
            socket=new Socket(host, port);
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            String line = null;
            while (isRunning && (line = reader.readLine()) != null){
                String[] split = line.split(",");
                sourceContext.collect(new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2])));
            }

            }

        @Override
        public void cancel() {
            isRunning=false;
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

