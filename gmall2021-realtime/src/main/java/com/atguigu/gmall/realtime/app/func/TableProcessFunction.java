package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * Author: Felix
 * Date: 2021/2/1
 * Desc:  配置表处理函数
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor = null;
    //定义 Phoenix 的连接
    private Connection connection = null;
    //因为要将维度数据通过侧输出流输出，所以我们在这里定义一个侧输出流标记
    private final OutputTag<JSONObject> outputTag;
    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化 Phoenix 的连接
        Class.forName(GmallConfig.PHOENIX_SERVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

        //实例化函数对象的时候，将侧输出流标签也进行赋值
    public TableProcessFunction(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }
    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        // 主流处理方案
            //过滤数据
            //根据状态分流
        //广播流广播出来的状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        //获取表名和操作类型
        String table = jsonObject.getString("table");
        String type = jsonObject.getString("type");
        String key = table + ":" + type;
        //取出对应的配置信息数据
        TableProcess tableProcess = broadcastState.get(key);
        if (tableProcess != null) {
            //向数据中追加 sink_table 信息
            jsonObject.put("sink_table", tableProcess.getSinkTable());
            //根据配置信息中提供的字段做数据过滤
            filterColumn(jsonObject.getJSONObject("data"), tableProcess.getSinkColumns());
            //判断当前数据应该写往 HBASE 还是 Kafka
            if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                //Kafka 数据,将数据输出到主流
                collector.collect(jsonObject);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                //HBase 数据,将数据输出到侧输出流
                readOnlyContext.output(outputTag, jsonObject);
            }
        } else {
            System.out.println("No Key " + key + " In Mysql!");
        }
    }

    @Override
    public void processBroadcastElement(String jsonStr, Context context, Collector<JSONObject> collector) throws Exception {
        // 广播流处理方案
            //检查hbase表是否存在并建表
            //解析数据,转换为TableProcess,写入状态供主流使用
        //创建状态对象
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        //将配置信息流中的数据转换为 JSON 对象  {"database":"","table":"","type","","data":{"":""}}
        JSONObject jsonObject = JSON.parseObject(jsonStr);
        //取出数据中的表名以及操作类型封装 key
        JSONObject data = jsonObject.getJSONObject("data");
        String table = data.getString("source_table");
        String type = data.getString("operate_type");
        String key = table + ":" + type;
        //取出 Value 数据封装为 TableProcess 对象
        TableProcess tableProcess = JSON.parseObject(data.toString(), TableProcess.class);
        // 如果是HBASE的配置,则建表
        if (tableProcess.getSinkType().equals(GmallConfig.HABSE_SCHEMA)){
            checkTable(tableProcess.getSinkTable(), tableProcess.getSinkColumns(), tableProcess.getSinkPk(), tableProcess.getSinkExtend());

        }
        System.out.println("Key:" + key + "," + tableProcess);
        //广播出去
        broadcastState.put(key, tableProcess);
    }


    /**
     * Phoenix 建表
     *
     * @param sinkTable   表名 test
     * @param sinkColumns 表名字段 id,name,sex
     * @param sinkPk      表主键 id
     * @param sinkExtend  表扩展字段 ""
     *                    create table if not exists mydb.test(id varchar primary key,name
     *                    varchar,sex varchar) ...
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        //给主键以及扩展字段赋默认值
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }
        //封装建表 SQL
        StringBuilder createSql = new StringBuilder("create table if not exists ").append(GmallConfig.HABSE_SCHEMA).append(".").append(sinkTable).append("(");
        //遍历添加字段信息
        String[] fields = sinkColumns.split(",");
        for (int i = 0; i < fields.length; i++) {
            //取出字段
            String field = fields[i];
            //判断当前字段是否为主键
            if (sinkPk.equals(field)) {
                createSql.append(field).append(" varchar primary key ");
            } else {
                createSql.append(field).append(" varchar ");
            }
            //如果当前字段不是最后一个字段,则追加","
            if (i < fields.length - 1) {
                createSql.append(",");
            }
        }
        createSql.append(")");
        createSql.append(sinkExtend);
        System.out.println(createSql);
        //执行建表 SQL
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(createSql.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("创建 Phoenix 表" + sinkTable + "失败！");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //根据配置信息中提供的字段做数据过滤
    private void filterColumn(JSONObject data, String sinkColumns) {
        //保留的数据字段
        String[] fields = sinkColumns.split(",");
        List<String> fieldList = Arrays.asList(fields);
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(next -> !fieldList.contains(next.getKey()));
    }



}
//    //因为要将维度数据通过侧输出流输出，所以我们在这里定义一个侧输出流标记
//    private OutputTag<JSONObject> outputTag;
//
//    //用于在内存中存放配置表信息的Map <表名：操作,tableProcess>
//    private Map<String, TableProcess> tableProcessMap = new HashMap<>();
//
//    //用于在内存中存放已经处理过的表（在phoenix中已经建过的表）
//    private Set<String> existsTables = new HashSet<>();
//
//    //声明Phoenix的连接对象
//    Connection conn = null;
//
//    //实例化函数对象的时候，将侧输出流标签也进行赋值
//    public TableProcessFunction(OutputTag<JSONObject> outputTag) {
//        this.outputTag = outputTag;
//    }
//
//    //在函数被调用的时候执行的方法，执行一次
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        //初始化Phoenix连接
//        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
//
//        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
//
//        //初始化配置表信息，里面会用到mysql
//        refreshMeta();
//
//        //开启一个定时任务
//        // 因为配置表的数据可能会发生变化，每隔一段时间就从配置表中查询一次数据，更新到map，并检查建表
//        //从现在起过delay毫秒后，每隔period执行一次
//        Timer timer = new Timer();
//        timer.schedule(new TimerTask() {
//            @Override
//            public void run() {
//                refreshMeta();
//            }
//        }, 5000, 5000);
//
//    }
//
//    private void refreshMeta() {
//        //========1.从MySQL数据库配置表中查询配置信息============
//        System.out.println("查询配置表信息");
//        List<TableProcess> tableProcessList = MySQLUtil.queryList("select * from table_process", TableProcess.class, true);
//        //对查询出来的结果集进行遍历
//        for (TableProcess tableProcess : tableProcessList) {
//            //获取源表表名
//            String sourceTable = tableProcess.getSourceTable();
//            //获取操作类型
//            String operateType = tableProcess.getOperateType();
//            //输出类型      hbase|kafka
//            String sinkType = tableProcess.getSinkType();
//            //输出目的地表名或者主题名
//            String sinkTable = tableProcess.getSinkTable();
//            //输出字段
//            String sinkColumns = tableProcess.getSinkColumns();
//            //表的主键
//            String sinkPk = tableProcess.getSinkPk();
//            //建表扩展语句
//            String sinkExtend = tableProcess.getSinkExtend();
//            //拼接保存配置的key
//            String key = sourceTable + ":" + operateType;
//
//            //========2.将从配置表中查询到配置信息，保存到内存的map集合中=============
//            tableProcessMap.put(key, tableProcess);
//
//            //========3.如果当前配置项是维度配置，需要向Hbase表中保存数据，那么我们需要判断phoenix中是否存在这张表=====================
//            if (TableProcess.SINK_TYPE_HBASE.equals(sinkType) && "insert".equals(operateType)) {
//                boolean notExist = existsTables.add(sourceTable);
//                //如果在内存Set集合中不存在这个表，那么在Phoenix中创建这种表
//                if (notExist) {
//                    //检查在Phonix中是否存在这种表
//                    //有可能已经存在，只不过是应用缓存被清空，导致当前表没有缓存，这种情况是不需要创建表的
//                    //在Phoenix中，表的确不存在，那么需要将表创建出来
//                    checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
//                }
//            }
//        }
//        //如果没有从数据库的配置表中读取到数据
//        if (tableProcessMap == null || tableProcessMap.size() == 0) {
//            throw new RuntimeException("没有从数据库的配置表中读取到数据");
//        }
//    }
//
//    private void checkTable(String tableName, String fields, String pk, String ext) {
//        //如果在配置表中，没有配置主键 需要给一个默认主键的值
//        if (pk == null) {
//            pk = "id";
//        }
//        //如果在配置表中，没有配置建表扩展 需要给一个默认建表扩展的值
//        if (ext == null) {
//            ext = "";
//        }
//        //拼接建表语句
//        StringBuilder createSql = new StringBuilder("create table if not exists " +
//            GmallConfig.HABSE_SCHEMA + "." + tableName + "(");
//
//        //对建表字段进行切分
//        String[] fieldsArr = fields.split(",");
//        for (int i = 0; i < fieldsArr.length; i++) {
//            String field = fieldsArr[i];
//            //判断当前字段是否为主键字段
//            if (pk.equals(field)) {
//                createSql.append(field).append(" varchar primary key ");
//            } else {
//                createSql.append("info.").append(field).append(" varchar ");
//            }
//            if (i < fieldsArr.length - 1) {
//                createSql.append(",");
//            }
//        }
//        createSql.append(")");
//        createSql.append(ext);
//
//        System.out.println("创建Phoenix表的语句:" + createSql);
//
//        //获取Phoenix连接
//        PreparedStatement ps = null;
//        try {
//            ps = conn.prepareStatement(createSql.toString());
//            ps.execute();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        } finally {
//            if (ps != null) {
//                try {
//                    ps.close();
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                    throw new RuntimeException("Phoenix建表失败");
//                }
//            }
//        }
//    }
//
//    //每过来一个元素，方法执行一次，主要任务是根据内存中配置表Map对当前进来的元素进行分流处理
//    @Override
//    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
//        //获取表名
//        String table = jsonObj.getString("table");
//        //获取操作类型
//        String type = jsonObj.getString("type");
//        //注意：问题修复  如果使用Maxwell的Bootstrap同步历史数据  ，这个时候它的操作类型叫bootstrap-insert
//        if ("bootstrap-insert".equals(type)) {
//            type = "insert";
//            jsonObj.put("type", type);
//        }
//
//        if (tableProcessMap != null && tableProcessMap.size() > 0) {
//            //根据表名和操作类型拼接key
//            String key = table + ":" + type;
//            //从内存的配置Map中获取当前key对象的配置信息，决定输出到哪个流
//            TableProcess tableProcess = tableProcessMap.get(key);
//            //如果获取到了该元素对应的配置信息
//            if (tableProcess != null) {
//                // 获取sinkTable，指明当前这条数据应该发往何处  如果是维度数据，
//                // 那么对应的是phoenix中的表名；如果是事实数据，对应的是kafka的主题名
//                jsonObj.put("sink_table", tableProcess.getSinkTable());// 获取目标表(或主题)
//                String sinkColumns = tableProcess.getSinkColumns();// 获取目标字段
//                //如果指定了sinkColumn，需要对保留的字段进行过滤处理
//                if (sinkColumns != null && sinkColumns.length() > 0) {
//                    filterColumn(jsonObj.getJSONObject("data"), sinkColumns);
//                }
//            } else {
//                System.out.println("NO this Key:" + key + "in MySQL");
//            }
//
//            //根据sinkType，将数据输出到不同的流
//            if(tableProcess != null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)){
//                //如果sinkType = hbase ，说明是维度数据，通过侧输出流输出
//                ctx.output(outputTag,jsonObj);
//            }else if(tableProcess != null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_KAFKA)){
//                //如果sinkType = kafka ，说明是事实数据，通过主流输出
//                out.collect(jsonObj);
//            }
//        }
//    }
//
//    //对Data中数据进行进行过滤
//    private void filterColumn(JSONObject data, String sinkColumns) {
//        //sinkColumns 表示要保留那些列     id,out_trade_no,order_id
//        String[] cols = sinkColumns.split(",");
//        //将数组转换为集合，为了判断集合中是否包含某个元素
//        List<String> columnList = Arrays.asList(cols);
//
//        //获取json对象中封装的一个个键值对   每个键值对封装为Entry类型
//        Set<Map.Entry<String, Object>> entrySet = data.entrySet();
//
//        Iterator<Map.Entry<String, Object>> it = entrySet.iterator();
//
//        for (;it.hasNext();) {
//            Map.Entry<String, Object> entry = it.next();
//            if(!columnList.contains(entry.getKey())){
//                it.remove();
//            }
//        }
//    }
//}