package day03.sink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;


public class Jdbs {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9988)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0],
                                Long.parseLong(split[1]),
                                Integer.parseInt(split[2]));
                    }
                });

        waterSensorDS.addSink(JdbcSink.sink(
                "insert into `test01` (`id`,`ts`,`vc`) values (?,?,?)",
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                        preparedStatement.setString(1,waterSensor.getId());
                        preparedStatement.setLong(2,waterSensor.getTs());
                        preparedStatement.setInt(3,waterSensor.getVc());
                    }
                },JdbcExecutionOptions.builder().withBatchSize(1).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://hadoop102:3306/test01?useSSL=false")
                .withDriverName("com.mysql.jdbc.Driver")
                .withUsername("root")
                .withPassword("123456")
                .build()
        ));
        env.execute();


    }
}
