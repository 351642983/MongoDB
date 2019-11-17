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
 
//mongodb �������ݿ⹤����
public class MongoDBUtil {
    //��ͨ����֤��ȡ�������ݿ����
    public static MongoDatabase getConnect(){
        //���ӵ� mongodb ����
        MongoClient mongoClient = new MongoClient("192.168.57.128", 27017);
 
        //���ӵ����ݿ�
        MongoDatabase mongoDatabase = mongoClient.getDatabase("student");
 
        //�����������ݿ����
        return mongoDatabase;
    }
    
  //ɾ����ɸѡ��ƥ��ĵ����ĵ�
    public void deleteOne(String table,String qian,String info){
        //��ȡ���ݿ����Ӷ���
        MongoDatabase mongoDatabase = MongoDBUtil.getConnect();
        //��ȡ����
        MongoCollection<Document> collection = mongoDatabase.getCollection(table);
        //����ɾ������
        Bson filter = Filters.eq(qian, info);
        //ɾ����ɸѡ��ƥ��ĵ����ĵ�
        collection.deleteOne(filter);
    }
    
  //����һ���ĵ�
    public <T> void insertOne(String table,T info){
        //��ȡ���ݿ����Ӷ���
        MongoDatabase mongoDatabase = MongoDBUtil.getConnect();
        //��ȡ����
        MongoCollection<Document> collection = mongoDatabase.getCollection(table);
        //Ҫ���������

        //����һ���ĵ�
        collection.insertOne(TtoDocument(info));
    }
    public void insetOne(String table,String []name,String []value)
    {
    	 //��ȡ���ݿ����Ӷ���
        MongoDatabase mongoDatabase = MongoDBUtil.getConnect();
        //��ȡ����
        MongoCollection<Document> collection = mongoDatabase.getCollection(table);
        //Ҫ���������

        //����һ���ĵ�
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
    
    
    
  //�������ĵ�
    public void insertMany(String table,List<Document> ld){
        //��ȡ���ݿ����Ӷ���
        MongoDatabase mongoDatabase = MongoDBUtil.getConnect();
        //��ȡ����
        MongoCollection<Document> collection = mongoDatabase.getCollection(table);
        //Ҫ���������
        //�������ĵ�
        collection.insertMany(ld);
    }
    
  
  //ɾ����ɸѡ��ƥ��������ĵ�
    public void deleteAll(String table,String qian,String info){
        //��ȡ���ݿ����Ӷ���
        MongoDatabase mongoDatabase = MongoDBUtil.getConnect();
        //��ȡ����
        MongoCollection<Document> collection = mongoDatabase.getCollection(table);
        //����ɾ������
        Bson filter = Filters.eq(qian, info);
        //ɾ����ɸѡ��ƥ��������ĵ�
        collection.deleteMany(filter);
    }
  //���Ҽ����е������ĵ�
    public List<String> findAll(String table){
        //��ȡ���ݿ����Ӷ���
        MongoDatabase mongoDatabase = MongoDBUtil.getConnect();
        //��ȡ����
        MongoCollection<Document> collection = mongoDatabase.getCollection(table);
        FindIterable findIterable = collection.find();
        MongoCursor cursor = findIterable.iterator();
        List<String> result=new ArrayList<String>();
        while (cursor.hasNext()) {
        	result.add(""+cursor.next());
        }
        return result;
    }
  //ָ����ѯ��������ѯ
    public List<String> filterFind(String table,String qian,String hou){
        //��ȡ���ݿ����Ӷ���
        MongoDatabase mongoDatabase = MongoDBUtil.getConnect();
        //��ȡ����
        MongoCollection<Document> collection = mongoDatabase.getCollection(table);
        //ָ����ѯ������
        Bson filter = Filters.eq(qian, hou);
        //ָ����ѯ��������ѯ
        FindIterable findIterable = collection.find(filter);
        MongoCursor cursor = findIterable.iterator();
        List<String> result=new ArrayList<String>();
        while (cursor.hasNext()) {
            result.add(""+cursor.next());
        }
        return result;
    }
    
  //ȡ����ѯ���ĵ�һ���ĵ�
    public String findOne(String table){
        //��ȡ���ݿ����Ӷ���
        MongoDatabase mongoDatabase = MongoDBUtil.getConnect();
        //��ȡ����
        MongoCollection<Document> collection = mongoDatabase.getCollection(table);
        //���Ҽ����е������ĵ�
        FindIterable findIterable = collection.find();
        //ȡ����ѯ���ĵ�һ���ĵ�
        Document document = (Document) findIterable.first();
        //��ӡ���
        return (""+document);
    }
  //�޸ĵ����ĵ�
    public void updateOne(String table,String qian,String info,String Zone,String whatname,String value){
        //��ȡ���ݿ����Ӷ���
        MongoDatabase mongoDatabase = MongoDBUtil.getConnect();
        //��ȡ����
        MongoCollection<Document> collection = mongoDatabase.getCollection(table);
        //�޸Ĺ�����
        Bson filter = Filters.eq(qian, info);
        //ָ���޸ĵĸ����ĵ�
        Document document = new Document(Zone, new Document(whatname,value));
        //�޸ĵ����ĵ�
        collection.updateOne(filter, document);
    }
  //�޸Ķ���ĵ� Zooe $set
    public void updateAll(String table,String qian,String info,String Zone,String whatname,String value){
        //��ȡ���ݿ����Ӷ���
        MongoDatabase mongoDatabase = MongoDBUtil.getConnect();
        //��ȡ����
        MongoCollection<Document> collection = mongoDatabase.getCollection(table);
        //�޸Ĺ�����
        Bson filter = Filters.eq(qian, info);
        //ָ���޸ĵĸ����ĵ�
        Document document = new Document(Zone, new Document(whatname,value));
        //�޸Ķ���ĵ�
        collection.updateMany(filter, document);
    }

    public static void main(String args[])
    {
    	getConnect();
    	MongoDBUtil mg=new MongoDBUtil();
    	System.out.print(mg.findAll("student"));
    }
 
    //��Ҫ������֤��ʽ����
//    public static MongoDatabase getConnect2(){
//        List<ServerAddress> adds = new ArrayList<>();
//        //ServerAddress()���������ֱ�Ϊ ��������ַ �� �˿�
//        ServerAddress serverAddress = new ServerAddress("localhost", 27017);
//        adds.add(serverAddress);
//        
//        List<MongoCredential> credentials = new ArrayList<>();
//        //MongoCredential.createScramSha1Credential()���������ֱ�Ϊ �û��� ���ݿ����� ����
//        MongoCredential mongoCredential = MongoCredential.createScramSha1Credential("username", "databaseName", "password".toCharArray());
//        credentials.add(mongoCredential);
//        
//        //ͨ��������֤��ȡMongoDB����
//        MongoClient mongoClient = new MongoClient(adds, credentials);
// 
//        //���ӵ����ݿ�
//        MongoDatabase mongoDatabase = mongoClient.getDatabase("test");
// 
//        //�����������ݿ����
//        return mongoDatabase;
//    }
}