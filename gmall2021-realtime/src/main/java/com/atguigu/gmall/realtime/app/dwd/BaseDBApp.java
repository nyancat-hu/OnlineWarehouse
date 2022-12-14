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
 * Desc: ?????????????????????DWD???
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.????????????
        //1.1 ???????????????????????????
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 ???????????????
        env.setParallelism(1);
        //1.3 ??????Checkpoint???????????????????????????
        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000);
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/checkpoint/basedbapp"));
        //????????????
        // ???????????????????????????Checkpoint???????????????????????????noRestart
        // ??????????????????Checkpoint??????????????????????????????????????????????????????   ??????Integer.MaxValue
        //env.setRestartStrategy(RestartStrategies.noRestart());

        //TODO 2.???Kafka???ODS???????????????
        String topic = "ods_base_db_m";
        String groupId = "base_db_app_group";

        //2.1 ?????????????????????Kafka????????????
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        // ????????????Kafka?????????
        DataStreamSource<String> jsonStrDS = env.addSource(kafkaSource);
        //TODO 3.???DS??????????????????????????????      String-->Json
        //jsonStrDS.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(JSON::parseObject);

        //TODO 4.???????????????ETL   ??????table?????? ?????? data?????????????????????<3  ??????????????????????????????
        SingleOutputStreamOperator<JSONObject> filteredDS = jsonObjDS.filter(
                jsonObj -> jsonObj.getString("table") != null
                        && jsonObj.getJSONObject("data") != null
                        && jsonObj.getString("data").length() > 3
        );



        //5.?????? MySQL CDC Source
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall2021-realtime")
                .tableList("gmall2021-realtime.table_process")
                .deserializer(new DebeziumDeserializationSchema<String>() {
                    //??????????????????
                    @Override
                    public void deserialize(SourceRecord sourceRecord, Collector<String> collector)
                            throws Exception {
                        //??????&??????
                        String topic = sourceRecord.topic();
                        String[] split = topic.split("\\.");
                        String db = split[1];
                        String table = split[2];
                        //????????????
                        Struct value = (Struct) sourceRecord.value();
                        Struct after = value.getStruct("after");
                        JSONObject data = new JSONObject();
                        if (after != null) {
                            Schema schema = after.schema();
                            for (Field field : schema.fields()) {
                                data.put(field.name(), after.get(field.name()));
                            }
                        }
                        //??????????????????
                        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
                        //?????? JSON ???????????????????????????
                        JSONObject result = new JSONObject();
                        result.put("database", db);
                        result.put("table", table);
                        result.put("type", operation.toString().toLowerCase());
                        result.put("data", data);
                        collector.collect(result.toJSONString());
                    }
                    //??????????????????
                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
        //6.?????? MySQL ??????,????????????Mysql???????????????
        DataStreamSource<String> tableProcessDS = env.addSource(sourceFunction);
        //7.??????????????????,String?????????????????????Map??? ??????,???+???????????? ????????????keyBy???,???????????????????????????
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("table-process-state", String.class, TableProcess.class);
        // ???????????????
        BroadcastStream<String> broadcastStream = tableProcessDS.broadcast(mapStateDescriptor);
        //8.?????????????????????????????????
        BroadcastConnectedStream<JSONObject, String> connectedStream = filteredDS.connect(broadcastStream);


        //TODO 5. ????????????  ?????????????????????????????????kafka???DWD??????????????????????????????????????????????????????Hbase
        //5.1???????????????Hbase?????????????????????
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE){};

        //5.2 ????????????????????????????????????????????????
        // ?????????????????????????????????mysql??????????????????????????????????????????????????????
        SingleOutputStreamOperator<JSONObject> kafkaDS = connectedStream.process(
            // ???????????????????????????????????????????????????mysql??????????????????????????????
            new TableProcessFunction(hbaseTag, mapStateDescriptor)
        );

        //5.3??????????????????    ??????Hbase?????????
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbaseTag);

        kafkaDS.print("??????>>>>");
        hbaseDS.print("??????>>>>");

        //TODO 6.????????????????????????Phoenix?????????????????????
        hbaseDS.addSink(new DimSink());

        //TODO 7.????????????????????????kafka???dwd???
        // ??????ods??????sink?????????dwd??????source(producer)
        FlinkKafkaProducer<JSONObject> kafkaSink = MyKafkaUtil.getKafkaSinkBySchema(
            new KafkaSerializationSchema<JSONObject>() {
                @Override
                public void open(SerializationSchema.InitializationContext context) throws Exception {
                    System.out.println("kafka?????????");
                }
                @Override
                public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long timestamp) {
                    //???????????????????????????????????????????????????????????????????????????KafkaProducer??????
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
