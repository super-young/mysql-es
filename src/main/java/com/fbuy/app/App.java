package com.fbuy.app;

import java.net.InetSocketAddress;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * Hello world!
 *
 */
public class App {
    // JDBC 驱动名及数据库 URL
    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost:3306/fg_dev?useSSL=false";

    // 数据库的用户名与密码，需要根据自己的设置
    static final String USER = "root";
    static final String PASS = "123456";
    public static void main1(String args[]) {
        // 创建链接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(AddressUtils.getHostIp(),
                11111), "example", "", "");
        int batchSize = 1000;
        int emptyCount = 0;
        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();
            int totalEmptyCount = 120;
            while (emptyCount < totalEmptyCount) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    emptyCount++;
                    System.out.println("empty count : " + emptyCount);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                } else {
                    emptyCount = 0;
                    // System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);
                    printEntry(message.getEntries());
                }

                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }

            System.out.println("empty too many times, exit");
        } finally {
            connector.disconnect();
        }
    }

    private static void printEntry(List<Entry> entrys) {
        for (Entry entry : entrys) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            EventType eventType = rowChage.getEventType();
            System.out.println(String.format("================>; binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));

            for (RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == EventType.DELETE) {
                    printColumn(rowData.getBeforeColumnsList());
                } else if (eventType == EventType.INSERT) {
                    printColumn(rowData.getAfterColumnsList());
                } else {
                    System.out.println("------->; before");
                    printColumn(rowData.getBeforeColumnsList());
                    System.out.println("------->; after");
                    printColumn(rowData.getAfterColumnsList());
                }
            }
        }
    }

    private static void printColumn(List<Column> columns) {
        for (Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }

    public static void  main(String args[])
    {
        try {
            List<Goods> goodsList = findGoods();
            Integer numOfCreated = batchInsertToES(goodsList);
            System.out.println(numOfCreated);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<Goods> findGoods()throws Exception
    {
        Connection conn = null;
        Statement stmt = null;
        // 注册 JDBC 驱动
        Class.forName(JDBC_DRIVER);

        // 打开链接
        conn = DriverManager.getConnection(DB_URL,USER,PASS);

        // 执行查询
        stmt = conn.createStatement();
        String sql;
        sql = "SELECT g.*,c.cat_name,b.brand_name FROM dsc_goods as g,dsc_category as c,dsc_brand as b WHERE g.cat_id=c.cat_id And g.brand_id=b.brand_id";
        ResultSet rs = stmt.executeQuery(sql);
        List<Goods> list = new ArrayList<Goods>();
        while (rs.next()){
            String name = rs.getString("goods_name");
            String desc = rs.getString("goods_desc");
            String catName = rs.getString("cat_name");
            String brandName = rs.getString("brand_name");
            Double price = rs.getDouble("shop_price");
            Integer id = rs.getInt("goods_id");
            Goods goods = new Goods();
            goods.setBrandName(brandName);
            goods.setCatName(catName);
            goods.setName(name);
            goods.setPrice(price);
            goods.setDesc(desc);
            goods.setId(id);
            list.add(goods);
        }
        // 完成后关闭
        rs.close();
        stmt.close();
        conn.close();
        return list;
    }

    private static int batchInsertToES(List<Goods> list) throws Exception
    {
        int batchSize = 100;
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));

        BulkRequest request = new BulkRequest();

        for(Goods goods: list) {
            Map<String,Object> property = new HashMap<String,Object>();
            property.put("name",goods.getName());
            property.put("brand",goods.getBrandName());
            property.put("category",goods.getCatName());
            property.put("desc",goods.getDesc());
            property.put("price",goods.getPrice());
            property.put("id",goods.getId());

            request.add(new IndexRequest("goods", "test", null)
                    .source(property));
        }
        request.timeout("10m");

        BulkResponse bulkResponse = client.bulk(request);
        Map<String,Integer> results = new HashMap<String, Integer>(3);
        results.put("update",0);
        results.put("delete",0);
        results.put("created",0);
        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            DocWriteResponse itemResponse = bulkItemResponse.getResponse();

            if (bulkItemResponse.isFailed()) {
                BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                System.out.println("one Document failed");
                System.out.println(failure.getMessage());
            }

            if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX) {
                IndexResponse indexResponse = (IndexResponse) itemResponse;
                results.put("created",results.get("created")+1);
                System.out.println("added one document");
            } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.UPDATE) {
                UpdateResponse updateResponse = (UpdateResponse) itemResponse;
                results.put("update",results.get("update")+1);

            } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.DELETE) {
                DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
                results.put("delete",results.get("delete")+1);
            }
        }
        return results.get("created");
    }

    private static void testes()
    {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));

        Map<String,Object> property = new HashMap<String,Object>();
        property.put("name","text");
        property.put("age",15);
        List<String> list = new ArrayList<String>();
        list.add("football");
        list.add("coding");
        list.add("pingpangball");
        property.put("hobbies",list);
//        Map<String,Object> mapping = new HashMap<String,Object>();
        IndexRequest request = new IndexRequest("test_idx","test",null);
        request.source(property);
        try {
            IndexResponse response = client.index(request);
            System.out.println(response);
            client.close();
        }catch (Exception e){
            System.out.println(e);
        }
    }
}
