package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.gmall.realtime.app.func.DimSink;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import com.google.gson.JsonObject;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nullable;
import java.sql.Connection;

/**
 * Author: Felix
 * Date: 2021/2/1
 * Desc: 准备业务数据的DWD层
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.准备环境
        //1.1 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并新度
        env.setParallelism(1);
        //1.3 开启Checkpoint，并设置相关的参数
        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000);
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/checkpoint/basedbapp"));
        //重启策略
        // 如果说没有开启重启Checkpoint，那么重启策略就是noRestart
        // 如果说没有开Checkpoint，那么重启策略会尝试自动帮你进行重启   重启Integer.MaxValue
        //env.setRestartStrategy(RestartStrategies.noRestart());

        //TODO 2.从Kafka的ODS层读取数据
        String topic = "ods_base_db_m";
        String groupId = "base_db_app_group";

        //2.1 通过工具类获取Kafka的消费者
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        // 创建一个Kafka输入流
        DataStreamSource<String> jsonStrDS = env.addSource(kafkaSource);
        //TODO 3.对DS中数据进行结构的转换      String-->Json
        //jsonStrDS.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(JSON::parseObject);

        //TODO 4.对数据进行ETL   如果table为空 或者 data为空，或者长度<3  ，将这样的数据过滤掉
        SingleOutputStreamOperator<JSONObject> filteredDS = jsonObjDS.filter(
                jsonObj -> jsonObj.getString("table") != null
                        && jsonObj.getJSONObject("data") != null
                        && jsonObj.getString("data").length() > 3
        );



        //5.创建 MySQL CDC Source
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall2021-realtime")
                .tableList("gmall2021-realtime.table_process")
                .deserializer(new DebeziumDeserializationSchema<String>() {
                    //反序列化方法
                    @Override
                    public void deserialize(SourceRecord sourceRecord, Collector<String> collector)
                            throws Exception {
                        //库名&表名
                        String topic = sourceRecord.topic();
                        String[] split = topic.split("\\.");
                        String db = split[1];
                        String table = split[2];
                        //获取数据
                        Struct value = (Struct) sourceRecord.value();
                        Struct after = value.getStruct("after");
                        JSONObject data = new JSONObject();
                        if (after != null) {
                            Schema schema = after.schema();
                            for (Field field : schema.fields()) {
                                data.put(field.name(), after.get(field.name()));
                            }
                        }
                        //获取操作类型
                        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
                        //创建 JSON 用于存放最终的结果
                        JSONObject result = new JSONObject();
                        result.put("database", db);
                        result.put("table", table);
                        result.put("type", operation.toString().toLowerCase());
                        result.put("data", data);
                        collector.collect(result.toJSONString());
                    }
                    //定义数据类型
                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
        //6.读取 MySQL 数据,创建一个Mysql配置输入流
        DataStreamSource<String> tableProcessDS = env.addSource(sourceFunction);
        //7.创建广播状态,String里面存的是状态Map的 主键,表+操作类型 由于不是keyBy流,这里的状态可被公用
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("table-process-state", String.class, TableProcess.class);
        // 创建广播流
        BroadcastStream<String> broadcastStream = tableProcessDS.broadcast(mapStateDescriptor);
        //8.将主流和广播流进行链接
        BroadcastConnectedStream<JSONObject, String> connectedStream = filteredDS.connect(broadcastStream);


        //TODO 5. 动态分流  事实表放到主流，写回到kafka的DWD层；如果维度表，通过侧输出流，写入到Hbase
        //5.1定义输出到Hbase的侧输出流标签
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE){};

        //5.2 此处进行分流，分出主流和侧输出流
        // 在下面方法中会定时读取mysql的配置信息，做到实时改变数据输出路径
        SingleOutputStreamOperator<JSONObject> kafkaDS = connectedStream.process(
            // 在下面的方法中，对传入的数据，依照mysql配置，进行广播和分流
            new TableProcessFunction(hbaseTag, mapStateDescriptor)
        );

        //5.3获取侧输出流    写到Hbase的数据
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbaseTag);

        kafkaDS.print("事实>>>>");
        hbaseDS.print("维度>>>>");

        //TODO 6.将维度数据保存到Phoenix对应的维度表中
        hbaseDS.addSink(new DimSink());

        //TODO 7.将事实数据写回到kafka的dwd层
        // 创建ods层的sink，亦为dwd层的source(producer)
        FlinkKafkaProducer<JSONObject> kafkaSink = MyKafkaUtil.getKafkaSinkBySchema(
            new KafkaSerializationSchema<JSONObject>() {
                @Override
                public void open(SerializationSchema.InitializationContext context) throws Exception {
                    System.out.println("kafka序列化");
                }
                @Override
                public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long timestamp) {
                    //该方法用于返回数据对象，对数据对象进行加工，返回给KafkaProducer使用
                    String sinkTopic = jsonObj.getString("sink_table");
                    JSONObject dataJsonObj = jsonObj.getJSONObject("data");
                    return new ProducerRecord<>(sinkTopic,dataJsonObj.toString().getBytes());
                }
            }
        );

        kafkaDS.addSink(kafkaSink);

        env.execute();
    }
}
