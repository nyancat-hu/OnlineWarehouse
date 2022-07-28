package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * Author: Felix
 * Date: 2021/2/4
 * Desc:  用户跳出行为过滤
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1  准备本地测试流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.2 设置并行度
        env.setParallelism(4);

        //1.3 设置Checkpoint
        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000);
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/checkpoint/uniquevisit"))

        //TODO 2.从kafka中读取数据
        String sourceTopic = "dwd_page_log";
        String groupId = "user_jump_detail_group";
        String sinkTopic = "dwm_user_jump_detail";

        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> dataStream = env.addSource(kafkaSource);



        //TODO 3.对读取到的数据进行结构的换换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = dataStream.map(jsonStr -> JSON.parseObject(jsonStr));

        //jsonObjDS.print("json>>>>>");
        //注意：从Flink1.12开始，默认的时间语义就是事件时间，不需要额外指定；如果是之前的版本，需要通过如下语句指定事件时间语义
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //TODO 4. 指定事件时间字段
        // 增加时间事件 水位线
        SingleOutputStreamOperator<JSONObject> jsonObjWithTSDS = jsonObjDS.assignTimestampsAndWatermarks(
            WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(
                new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                        return jsonObj.getLong("ts");
                    }
                }
            ));

        //TODO 5.按照mid进行分组
        KeyedStream<JSONObject, String> keyByMidDS = jsonObjWithTSDS.keyBy(
            jsonObj -> jsonObj.getJSONObject("common").getString("mid")
        );

        /*
            计算页面跳出明细，需要满足两个条件
                1.不是从其它页面跳转过来的页面，是一个首次访问页面
                        last_page_id == null
                2.距离首次访问结束后10秒内，没有对其它的页面再进行访问
                    情况一：10s内，先进行了一次首次访问，再进行一次首次访问，则认为第一次跳出了
                    情况二：10s内，进行了一次首次访问，没有第二次访问了，则认为跳出了
        */
        //TODO 6.配置CEP表达式
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("begin").where(new SimpleCondition<JSONObject>() {
               @Override
               public boolean filter(JSONObject jsonObject) throws Exception {
                   String lastPageId =
                           jsonObject.getJSONObject("page").getString("last_page_id");
                   return lastPageId == null || lastPageId.length() <= 0;
               }
        }).times(2) //重复两次，默认的使用宽松近邻
                .consecutive() //指定使用严格近邻
                .within(Time.seconds(10));


        //TODO 7.根据：CEP表达式筛选流
        PatternStream<JSONObject> patternStream = CEP.pattern(keyByMidDS, pattern);

        //TODO 8.从筛选之后的流中，提取数据   将超时数据  放到侧输出流中
        OutputTag<String> timeoutTag = new OutputTag<String>("timeout"){};

        SingleOutputStreamOperator<String> filterDS = patternStream.flatSelect(
            timeoutTag,
            //处理超时数据
            new PatternFlatTimeoutFunction<JSONObject, String>() {
                @Override
                public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                    //第一条数据匹配上了，第二条数据没来，才会产生超时
                    //直接拿出第一个时间返回就行了

                    //获取所有符合begin的json对象
                    List<JSONObject> jsonObjectList = pattern.get("begin");
                    //注意：在timeout方法中的数据都会被参数1中的标签标记
                    for (JSONObject jsonObject : jsonObjectList) {
                        out.collect(jsonObject.toJSONString());
                        // 输出到侧输出流，也许是
                    }
                }
            },
            //处理的没有超时数据
            new PatternFlatSelectFunction<JSONObject, String>() {
                @Override
                public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<String> out) throws Exception {
                    //没有超时的数据，如果匹配上了我们的模式，说明跳出了
                    // 也就是说第一个数据为NULL,第二个的上一跳也为NULL，那我们提取第一个事件
                    // 事件匹配上以后，走这条线，其余事件就会被过滤掉

                    //获取所有符合begin的json对象
                    List<JSONObject> jsonObjectList = pattern.get("begin");
                    //注意：在timeout方法中的数据都会被参数1中的标签标记
                    for (JSONObject jsonObject : jsonObjectList) {
                        out.collect(jsonObject.toJSONString());
                        // 输出到主流
                    }
                }
            }
        );


        //TODO 9.从侧输出流中获取超时数据，合并主流和侧输出流
        DataStream<String> jumpDS = filterDS.getSideOutput(timeoutTag);


        //TODO 10.将跳出数据写回到kafka的DWM层
        jumpDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        env.execute();
    }
}
