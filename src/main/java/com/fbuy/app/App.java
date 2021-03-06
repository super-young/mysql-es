package com.fbuy.app;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.*;
import java.util.*;

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

import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;


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
    static final String SchemaName = "fg_dev";
    static Map<String,Map<String,ColumnMapConfig>> tablesMap=new HashMap<String,Map<String,ColumnMapConfig>>();
    static Map<String,List<String>> syncMap = new HashMap<String, List<String>>();
    private static RestHighLevelClient client=null;
    public static void main(String args[]) {
        init();

        try {
            List<Map<String,Object>> list =find("dsc_goods",tablesMap.get("dsc_goods"),"");
            System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
        initES();
        if(args.length>1){
            dumpGoodsToES();
            System.exit(0);
        }

        // 创建链接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(AddressUtils.getHostIp(),
                11111), "example", "", "");
        int batchSize = 1000;
        int emptyCount = 0;
        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();
            int totalEmptyCount = 1200;
            while (emptyCount < totalEmptyCount) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    emptyCount++;
                    System.out.println("empty count : " + emptyCount);
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                    }
                } else {
                    emptyCount = 0;
                    // System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);
                    try{
                        printEntry(message.getEntries());
                    }catch (Exception e){

                    }
                }

//                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }

            System.out.println("empty too many times, exit");
        } finally {
            connector.disconnect();
        }
    }

    public static void init()
    {
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document d = db.parse("config.xml");
            NodeList tables = d.getElementsByTagName("table");
            for(int i=0;i<tables.getLength();i++){
                Node  table = tables.item(i);
                String tableName = table.getAttributes().getNamedItem("name").getNodeValue();
                NodeList columns = table.getChildNodes().item(1).getChildNodes();
                tablesMap.put(tableName,parseTableColumns(columns));
            }
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected static Map<String,ColumnMapConfig> parseTableColumns(NodeList columns)
    {
        Map<String,ColumnMapConfig> map =new HashMap<String, ColumnMapConfig>();
        for(int i=0;i<columns.getLength();i++){
            if(columns.item(i).getNodeType() != Node.TEXT_NODE){
                NamedNodeMap attrs = columns.item(i).getAttributes();

                NamedNodeMap configMap = columns.item(i).getChildNodes().item(1).getAttributes();
                ColumnMapConfig cfg = new ColumnMapConfig();
                cfg.setType(configMap.getNamedItem("type").getNodeValue());
                if(cfg.getType().equals("related")) {
                    cfg.setTable(configMap.getNamedItem("table").getNodeValue());
                    cfg.setJoin(configMap.getNamedItem("join").getNodeValue());
                    cfg.setColumn(configMap.getNamedItem("column").getNodeValue());
                    cfg.setAs(configMap.getNamedItem("as").getNodeValue());
                }else if (cfg.getType().equals("embed")){
                    cfg.setTable(configMap.getNamedItem("table").getNodeValue());
                    cfg.setJoin(configMap.getNamedItem("join").getNodeValue());
                    cfg.setColumn(configMap.getNamedItem("column").getNodeValue());
                    cfg.setAs(configMap.getNamedItem("as").getNodeValue());
                    cfg.setKey(configMap.getNamedItem("key").getNodeValue());
                    cfg.setLeftKey(configMap.getNamedItem("leftKey").getNodeValue());
                }
                map.put(attrs.getNamedItem("name").getNodeValue(),cfg);
            }
        }
        return map;
    }

    private static void printEntry(List<Entry> entrys) throws IOException {

        for (Entry entry : entrys) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

//            if(!entry.getHeader().getSchemaName().equals(SchemaName) || !tablesMap.contains(entry.getHeader().getTableName())){
//                continue;
//            }

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
                    Map<String,Object> all = columnsAllToMap(rowData.getBeforeColumnsList());
                    String id = (String) all.get("goods_id");
                    deleteRowInES("goods","test",id);
                } else if (eventType == EventType.INSERT) {
                    Map<String,Object> all = columnsAllToMap(rowData.getAfterColumnsList());
                    String id = (String) all.get("goods_id");
                    addRowToES("goods","test",id,all);
                } else {
                    Map<String,Object> all = columnsAllToMap(rowData.getAfterColumnsList());
                    Map<String,Object> updated = columnsUpdatedToMap(rowData.getAfterColumnsList());
                    String id = (String) all.get("goods_id");
                    updateRowInES("goods","type",id,updated);
                }
            }
        }
    }

    private static Map<String,Object> columnsAllToMap(List<Column> cols)
    {
        Map<String,Object> map = new HashMap<String, Object>();
        for(Column col :cols){
            map.put(col.getName(),col.getValue());
        }
        return map;
    }

    private static Map<String,Object> columnsUpdatedToMap(List<Column> cols)
    {
        Map<String,Object> map = new HashMap<String, Object>();
        for(Column col :cols){
            if(col.hasUpdated())
                map.put(col.getName(),col.getValue());
        }
        return map;
    }

    private static void deleteRowInES(String index, String type, String id) throws IOException {
        DeleteRequest request = new DeleteRequest(index,type,id);
        client.delete(request);
    }

    private static void addRowToES(String index, String type, String id,Map<String,Object> goods) throws IOException {
        IndexRequest request = new IndexRequest(
                index,
                type,
                id).source(goods);
        client.index(request);
    }

    private static void updateRowInES(String index, String type, String id,Map<String,Object> goods_updated) throws IOException {
        UpdateRequest request = new UpdateRequest(
                index,
                type,
                id).doc(goods_updated);
        client.update(request);
    }

    private static void printColumn(List<Column> columns) {
        for (Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }

    public static void  dumpGoodsToES()
    {
        try {

            System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void initES()
    {
         client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));

    }

    private static List<Map<String,Object>> find(String table,Map<String,ColumnMapConfig> config,String condtion)throws Exception
    {
        Connection conn = null;
        Statement stmt = null;
        // 注册 JDBC 驱动
        Class.forName(JDBC_DRIVER);

        // 打开链接
        conn = DriverManager.getConnection(DB_URL,USER,PASS);

        // 执行查询
        stmt = conn.createStatement();
        StringBuffer select = new StringBuffer();
        select.append("SELECT ");
        StringBuffer where = new StringBuffer();
        if(condtion.length()>0){
            where.append(" ").append(condtion).append(" AND ");
        }
        StringBuffer from =new StringBuffer();
        from.append(table).append(",");
        for(Map.Entry<String,ColumnMapConfig> entry:config.entrySet()){
            String key = entry.getKey();
            ColumnMapConfig cfg = entry.getValue();
            if(cfg.getType().equals("plain")){
                select.append(table).append(".").append(key).append(",");
            }else if(cfg.getType().equals("related")){
                select.append(table).append(".").append(key).append(",");
                select.append(cfg.getTable()).append(".").append(cfg.getColumn()).append(",");
                where .append(table).append(".").append(key).append("=").append(cfg.getTable()).append(".").append(cfg.getJoin()).append(" AND ");
                from.append(cfg.getTable()).append(",");
            }else if(cfg.getType().equals("embed")){
                select.append(cfg.getTable()).append(".").append(cfg.getColumn()).append(",");
                from.append(cfg.getTable()).append(",");
                where.append(cfg.getTable()).append(".").append(cfg.getJoin()).append("=").append(table).append(".").
                        append(cfg.getLeftKey()).append(" AND ");
            }
        }
        StringBuffer sql = new StringBuffer();
        sql.append(select.substring(0,select.length()-1));
        sql.append(" FROM ").append(from.substring(0,from.length()-1));
        sql.append(" WHERE ").append(where.substring(0,where.length()-4));

        ResultSet rs = stmt.executeQuery(sql.toString());
        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
        ResultSetMetaData meteData = rs.getMetaData();
        Map<String,String> meteMap = new HashMap<String, String>();
        for (int i=1;i<=meteData.getColumnCount();i++){
            meteMap.put(meteData.getColumnName(i),meteData.getColumnTypeName(i));
        }
        while (rs.next()){
            Map<String,Object> map = new HashMap<String, Object>();
            for(Map.Entry<String,ColumnMapConfig> entry:config.entrySet()) {
                String key = entry.getKey();
                ColumnMapConfig cfg = entry.getValue();
                if(cfg.getType().equals("plain")) {
                    if(meteMap.get(key).matches("VARCHAR|TEXT")){
                        map.put(key,rs.getString(key));
                    }
                    if(meteMap.get(key).matches("INT|BIT")){
                        map.put(key,Integer.toString(rs.getInt(key)));
                    }
                    if(meteMap.get(key).matches("DOUBLE|DECIMAL")){
                        map.put(key,Double.toString(rs.getDouble(key)));
                    }
                }else if(cfg.getType().equals("relate")){
                    if(meteMap.get(key).matches("VARCHAR|TEXT")){
                        map.put(key,rs.getString(key));
                    }
                    if(meteMap.get(key).matches("INT|BIT")){
                        map.put(key,Integer.toString(rs.getInt(key)));
                    }
                    if(meteMap.get(key).matches("DOUBLE|DECIMAL")){
                        map.put(key,Double.toString(rs.getDouble(key)));
                    }
                    if(meteMap.get(cfg.getColumn()).matches("VARCHAR|TEXT")){
                        map.put(cfg.getAs(),rs.getString(cfg.getColumn()));
                    }
                    if(meteMap.get(cfg.getColumn()).matches("INT|BIT")){
                        map.put(cfg.getAs(),Integer.toString(rs.getInt(cfg.getColumn())));
                    }
                    if(meteMap.get(cfg.getColumn()).matches("DOUBLE|DECIMAL")){
                        map.put(cfg.getAs(),Double.toString(rs.getDouble(cfg.getColumn())));
                    }
                }else if(cfg.getType().equals("embed")){
                    Map<String,String> embedMap = new HashMap<String, String>();
                    String[] columns = cfg.getColumn().split(",");
                    for (String str:columns){
                        if(meteMap.get(str).matches("VARCHAR|TEXT")){
                            embedMap.put(str,rs.getString(str));
                        }
                        if(meteMap.get(str).matches("INT|BIT")){
                            embedMap.put(str,Integer.toString(rs.getInt(str)));
                        }
                        if(meteMap.get(str).matches("DOUBLE|DECIMAL")){
                            embedMap.put(str,Double.toString(rs.getDouble(str)));
                        }
                    }
                    map.put(cfg.getAs(),embedMap);
                }
            }
            list.add(map);
        }
        // 完成后关闭
        rs.close();
        stmt.close();
        conn.close();
        return list;
    }

    private static int batchInsertToES(String index,String type,String primaryKey,List<Map<String,Object>> docs) throws Exception
    {
        int batchSize = 100;

        try {
            BulkRequest request = new BulkRequest();

            for (Map<String,Object> doc : docs) {
                Map<String, Object> property = new HashMap<String, Object>();

                request.add(new IndexRequest(index, type, (String) doc.get(primaryKey))
                        .source(property));
            }
            request.timeout("2m");

            BulkResponse bulkResponse = client.bulk(request);
            Map<String, Integer> results = new HashMap<String, Integer>(3);
            results.put("update", 0);
            results.put("delete", 0);
            results.put("created", 0);
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                DocWriteResponse itemResponse = bulkItemResponse.getResponse();

                if (bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                    System.out.println("one Document failed");
                    System.out.println(failure.getMessage());
                }

                if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX) {
                    IndexResponse indexResponse = (IndexResponse) itemResponse;
                    results.put("created", results.get("created") + 1);
                    System.out.println("added one document");
                } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.UPDATE) {
                    UpdateResponse updateResponse = (UpdateResponse) itemResponse;
                    results.put("update", results.get("update") + 1);

                } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.DELETE) {
                    DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
                    results.put("delete", results.get("delete") + 1);
                }
                return results.get("created");
            }
        }catch (Exception e){

        }finally {
            client.close();

        }
        return 0;
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
