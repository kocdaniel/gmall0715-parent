package com.atguigu.gmall0715.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall0715.common.constant.GmallConstant;

import java.util.List;

public class CanalHandler {

    CanalEntry.EventType eventType;

    String tableName;

    List<CanalEntry.RowData> rowDataList;

    public CanalHandler(CanalEntry.EventType eventType,
                        String tableName,
                        List<CanalEntry.RowData> rowDataList) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDataList = rowDataList;
    }

    public void handler(){
        // 判断业务类型：如下单
        if(tableName.equals("order_info") && eventType == CanalEntry.EventType.INSERT && rowDataList.size() > 0){
            sendToKafka(GmallConstant.KAFKA_TOPIC_ORDER);
        }

    }

    public void sendToKafka(String topic){

        // RowData : 出现变化的数据行信息
        for (CanalEntry.RowData rowData : rowDataList) {
            // afterColumnList (修改后)
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();

            // 将一行中的所有列数据放入一个jsonObject对象中
            for (CanalEntry.Column column : afterColumnsList) {
                jsonObject.put(column.getName(), column.getValue());
            }
            // 发送到kafka
            KafkaSender.send(topic, jsonObject.toJSONString());
        }
    }

}
