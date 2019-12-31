package com.atguigu.gmall0715.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalApp {
    public static void main(String[] args) {
        // CanalClient
        // TODO 1 连接canal服务端
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop102", 11111),
                "example",
                "",
                ""
        );

        // TODO 2 抓取数据
        while (true){
            canalConnector.connect();
            // 抓取表
            canalConnector.subscribe("gmall0715.*");
            //一个message=一次抓取  一次抓取可以抓多个sql的执行结果集
            Message message = canalConnector.get(100);

            List<CanalEntry.Entry> entries = message.getEntries();

            if (entries.size() == 0){
                System.out.println("没有数据，休息5s");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{
                // TODO 3 抓取数据后，提取数据，一个entry代表一个sql的执行结果
                for (CanalEntry.Entry entry : entries) {

                    // 过滤掉不需要的数据类型
                    if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA){
                        continue;
                    }
                    // 取出序列化后的值集合
                    ByteString storeValue = entry.getStoreValue();
                    CanalEntry.RowChange rowChange = null;

                    try {
                        // 反序列化
                        // RowChange是把entry中的storeValue反序列化的对象
                        rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }

                    if (rowChange != null){
                        // 行集
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        // 获取数据所属于的表的信息
                        String tableName = entry.getHeader().getTableName();
                        // 获取数据的操作类型
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        CanalHandler canalHandler = new CanalHandler(eventType, tableName, rowDatasList);
                        canalHandler.handler();
                    }
                }
            }
        }

    }
}
