package hbase;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
 
//mongodb 连接数据库工具类
public class MongoDBUtil {
    //不通过认证获取连接数据库对象
    public static MongoDatabase getConnect(){
        //连接到 mongodb 服务
        MongoClient mongoClient = new MongoClient("192.168.57.128", 27017);
 
        //连接到数据库
        MongoDatabase mongoDatabase = mongoClient.getDatabase("student");
 
        //返回连接数据库对象
        return mongoDatabase;
    }
    
  //删除与筛选器匹配的单个文档
    public void deleteOne(String table,String qian,String info){
        //获取数据库连接对象
        MongoDatabase mongoDatabase = MongoDBUtil.getConnect();
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection(table);
        //申明删除条件
        Bson filter = Filters.eq(qian, info);
        //删除与筛选器匹配的单个文档
        collection.deleteOne(filter);
    }
    
  //插入一个文档
    public <T> void insertOne(String table,T info){
        //获取数据库连接对象
        MongoDatabase mongoDatabase = MongoDBUtil.getConnect();
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection(table);
        //要插入的数据

        //插入一个文档
        collection.insertOne(TtoDocument(info));
    }
    public void insetOne(String table,String []name,String []value)
    {
    	 //获取数据库连接对象
        MongoDatabase mongoDatabase = MongoDBUtil.getConnect();
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection(table);
        //要插入的数据

        //插入一个文档
        collection.insertOne(toDocument(name,value));
    }
    
    public <T> Document TtoDocument(T info)
    {
    	EntityToString ets=new EntityToString();
    	List<String> nalist=ets.getNameList(info.getClass());
    	Document document=new Document();
    	for(int i=0;i<nalist.size();i++)
    	{
    		document.append(nalist.get(i), ets.getNameValue(info,nalist.get(i) ));
    	}
    	return document;
    }
    public Document toDocument(String []name,String []value)
    {
    	Document document=new Document();
    	for(int i=0;i<name.length;i++)
    	{
    		document.append(name[i], value[i]);
    	}
    	return document;
    }
    
    
    
  //插入多个文档
    public void insertMany(String table,List<Document> ld){
        //获取数据库连接对象
        MongoDatabase mongoDatabase = MongoDBUtil.getConnect();
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection(table);
        //要插入的数据
        //插入多个文档
        collection.insertMany(ld);
    }
    
  
  //删除与筛选器匹配的所有文档
    public void deleteAll(String table,String qian,String info){
        //获取数据库连接对象
        MongoDatabase mongoDatabase = MongoDBUtil.getConnect();
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection(table);
        //申明删除条件
        Bson filter = Filters.eq(qian, info);
        //删除与筛选器匹配的所有文档
        collection.deleteMany(filter);
    }
  //查找集合中的所有文档
    public List<String> findAll(String table){
        //获取数据库连接对象
        MongoDatabase mongoDatabase = MongoDBUtil.getConnect();
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection(table);
        FindIterable findIterable = collection.find();
        MongoCursor cursor = findIterable.iterator();
        List<String> result=new ArrayList<String>();
        while (cursor.hasNext()) {
        	result.add(""+cursor.next());
        }
        return result;
    }
  //指定查询过滤器查询
    public List<String> filterFind(String table,String qian,String hou){
        //获取数据库连接对象
        MongoDatabase mongoDatabase = MongoDBUtil.getConnect();
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection(table);
        //指定查询过滤器
        Bson filter = Filters.eq(qian, hou);
        //指定查询过滤器查询
        FindIterable findIterable = collection.find(filter);
        MongoCursor cursor = findIterable.iterator();
        List<String> result=new ArrayList<String>();
        while (cursor.hasNext()) {
            result.add(""+cursor.next());
        }
        return result;
    }
    
  //取出查询到的第一个文档
    public String findOne(String table){
        //获取数据库连接对象
        MongoDatabase mongoDatabase = MongoDBUtil.getConnect();
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection(table);
        //查找集合中的所有文档
        FindIterable findIterable = collection.find();
        //取出查询到的第一个文档
        Document document = (Document) findIterable.first();
        //打印输出
        return (""+document);
    }
  //修改单个文档
    public void updateOne(String table,String qian,String info,String Zone,String whatname,String value){
        //获取数据库连接对象
        MongoDatabase mongoDatabase = MongoDBUtil.getConnect();
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection(table);
        //修改过滤器
        Bson filter = Filters.eq(qian, info);
        //指定修改的更新文档
        Document document = new Document(Zone, new Document(whatname,value));
        //修改单个文档
        collection.updateOne(filter, document);
    }
  //修改多个文档 Zooe $set
    public void updateAll(String table,String qian,String info,String Zone,String whatname,String value){
        //获取数据库连接对象
        MongoDatabase mongoDatabase = MongoDBUtil.getConnect();
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection(table);
        //修改过滤器
        Bson filter = Filters.eq(qian, info);
        //指定修改的更新文档
        Document document = new Document(Zone, new Document(whatname,value));
        //修改多个文档
        collection.updateMany(filter, document);
    }

    public static void main(String args[])
    {
    	getConnect();
    	MongoDBUtil mg=new MongoDBUtil();
    	System.out.print(mg.findAll("student"));
    }
 
    //需要密码认证方式连接
//    public static MongoDatabase getConnect2(){
//        List<ServerAddress> adds = new ArrayList<>();
//        //ServerAddress()两个参数分别为 服务器地址 和 端口
//        ServerAddress serverAddress = new ServerAddress("localhost", 27017);
//        adds.add(serverAddress);
//        
//        List<MongoCredential> credentials = new ArrayList<>();
//        //MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码
//        MongoCredential mongoCredential = MongoCredential.createScramSha1Credential("username", "databaseName", "password".toCharArray());
//        credentials.add(mongoCredential);
//        
//        //通过连接认证获取MongoDB连接
//        MongoClient mongoClient = new MongoClient(adds, credentials);
// 
//        //连接到数据库
//        MongoDatabase mongoDatabase = mongoClient.getDatabase("test");
// 
//        //返回连接数据库对象
//        return mongoDatabase;
//    }
}