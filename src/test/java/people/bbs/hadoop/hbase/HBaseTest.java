package people.bbs.hadoop.hbase;

import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import people.bbs.hadoop.hbase.service.HBaseService;
import people.bbs.hadoop.hbase.util.HBaseUtil;

@RunWith(SpringJUnit4ClassRunner.class) 
@ContextConfiguration(locations = {"classpath*:applicationContext.xml"}) 
public class HBaseTest {

//	// 声明静态配置，配置zookeeper
//	static Configuration configuration = null;
//	static Connection connection = null;
	static String hbaseUrl = "master:2181,slave01:2181,slave02:2181";
	static String tableName = "t_table";
//	static {
//		configuration = HBaseConfiguration.create();
//		configuration.set("hbase.zookeeper.quorum", "master:2181,slave01:2181,slave02:2181");
//		try {
//			connection = ConnectionFactory.createConnection(configuration);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
	
	@Autowired
	private HBaseService hBaseService;

	/**
	 * 创建表
	 * 
	 * @param tableName
	 */
	@Test
	public void createTable() {
		
		HBaseUtil.init(hbaseUrl);
		System.out.println("start create table ......");
		try {
			
			hBaseService.createTable(tableName,  new String[] { "f1", "f2", "f3" }, false);
		} catch (Exception e) {
			e.printStackTrace();
		}
//		try {
//			Admin admin = connection.getAdmin();
//			TableName tableName = TableName.valueOf(tableStr);
//			if (admin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建
//				admin.disableTable(tableName);
//				admin.deleteTable(tableName);
//				System.out.println(tableName + " is exist,detele....");
//			}
//			TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tableName);
//			// 添加表列信息
//			if (familyNames != null && familyNames.length > 0) {
//				for (String familyName : familyNames) {
//					tableDescriptor.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(familyName)).build());
//				}
//			}
//			admin.createTable(tableDescriptor.build());
//		} catch (MasterNotRunningException e) {
//			e.printStackTrace();
//		} catch (ZooKeeperConnectionException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		System.out.println("end create table ......");
	}

	
//	/**
//	 * 添加行列数据数据
//	 * 
//	 * @param tableName
//	 * @throws Exception
//	 */
//	public static void insertData(String tableName, String rowId, String familyName, String qualifier, String value) throws Exception {
//		System.out.println("start insert data ......");
//		Table table = connection.getTable(TableName.valueOf(tableName));
//		Put put = new Put(rowId.getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
//		put.addColumn(familyName.getBytes(), qualifier.getBytes(), value.getBytes());// 本行数据的第一列
//		try {
//			table.put(put);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		System.out.println("end insert data ......");
//	}
//
//	/**
//	 * 删除行
//	 * 
//	 * @param tablename
//	 * @param rowkey
//	 */
//	public static void deleteRow(String tablename, String rowkey) {
//		try {
//			Table table = connection.getTable(TableName.valueOf(tablename));
//			Delete d1 = new Delete(rowkey.getBytes());
//			table.delete(d1);// d1.addColumn(family,
//								// qualifier);d1.addFamily(family);
//			System.out.println("删除行成功!");
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
//
	/**
	 * 查询所有数据
	 * 
	 * @param tableName
	 * @throws Exception
	 */
	@Test
	public void queryAll() throws Exception {
		 HBaseUtil.init(hbaseUrl);
		 hBaseService.queryAll(tableName);
	}
//
//	/**
//	 * 根据rowId查询
//	 * 
//	 * @param tableName
//	 * @throws Exception
//	 */
//	public static void queryByRowId(String tableName, String rowId) throws Exception {
//		Table table = connection.getTable(TableName.valueOf(tableName));
//		try {
//			Get scan = new Get(rowId.getBytes());// 根据rowkey查询
//			Result r = table.get(scan);
//			System.out.println("获得到rowkey:" + new String(r.getRow()));
//			for (Cell keyValue : r.rawCells()) {
//				System.out.println("列：" + new String(CellUtil.cloneFamily(keyValue)) + ":" + new String(CellUtil.cloneQualifier(keyValue)) + "====值:" + new String(CellUtil.cloneValue(keyValue)));
//			}
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
//
//	/**
//	 * 根据列条件查询
//	 * 
//	 * @param tableName
//	 */
//	public static void queryByCondition(String tableName, String familyName, String qualifier, String value) {
//
//		try {
//			Table table = connection.getTable(TableName.valueOf(tableName));
//			Filter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes(qualifier), CompareOp.EQUAL, Bytes.toBytes(value)); // 当列familyName的值为value时进行查询
//			Scan s = new Scan();
//			s.setFilter(filter);
//			ResultScanner rs = table.getScanner(s);
//			for (Result r : rs) {
//				System.out.println("获得到rowkey:" + new String(r.getRow()));
//				for (Cell keyValue : r.rawCells()) {
//					System.out.println("列：" + new String(CellUtil.cloneFamily(keyValue)) + ":" + new String(CellUtil.cloneQualifier(keyValue)) + "====值:" + new String(CellUtil.cloneValue(keyValue)));
//				}
//			}
//			rs.close();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//	}
//
//	/**
//	 * 多条件查询
//	 * 
//	 * @param tableName
//	 */
//	public static void queryByConditions(String tableName, String[] familyNames, String[] qualifiers, String[] values) {
//
//		try {
//			Table table = connection.getTable(TableName.valueOf(tableName));
//			List<Filter> filters = new ArrayList<Filter>();
//			if (familyNames != null && familyNames.length > 0) {
//				int i = 0;
//				for (String familyName : familyNames) {
//					Filter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes(qualifiers[i]), CompareOp.EQUAL, Bytes.toBytes(values[i]));
//					filters.add(filter);
//					i++;
//				}
//			}
//			FilterList filterList = new FilterList(filters);
//			Scan scan = new Scan();
//			scan.setFilter(filterList);
//			ResultScanner rs = table.getScanner(scan);
//			for (Result r : rs) {
//				System.out.println("获得到rowkey:" + new String(r.getRow()));
//				for (Cell keyValue : r.rawCells()) {
//					System.out.println("列：" + new String(CellUtil.cloneFamily(keyValue)) + ":" + new String(CellUtil.cloneQualifier(keyValue)) + "====值:" + new String(CellUtil.cloneValue(keyValue)));
//				}
//			}
//			rs.close();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//
//	/**
//	 * 删除表
//	 * 
//	 * @param tableName
//	 */
//	public static void dropTable(String tableStr) {
//		try {
//			Admin admin = connection.getAdmin();
//			TableName tableName = TableName.valueOf(tableStr);
//			admin.disableTable(tableName);
//			admin.deleteTable(tableName);
//			admin.close();
//		} catch (MasterNotRunningException e) {
//			e.printStackTrace();
//		} catch (ZooKeeperConnectionException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}

//	public static void main(String[] args) throws Exception {
//		// 创建表
//		createTable("t_table", new String[] { "f1", "f2", "f3" });
////		// 添加数据
//		insertData("t_table", "row-0001", "f1", "a", "fffaaa");
//		insertData("t_table", "row-0001", "f2", "b", "fffbbb");
//		insertData("t_table", "row-0001", "f3", "c", "fffccc");
//		insertData("t_table", "row-0002", "f1", "a", "eeeeee");
////		// 查询全部数据
////		queryAll("t_table");
////		// 根据rowid查询数据
////		queryByRowId("t_table", "row-0001");
////		// 列条件查询
////		queryByCondition("t_table", "f1", "a", "eeeeee");
////		// 多条件查询
////		queryByConditions("t_table", new String[] { "f1", "f3" }, new String[] { "a", "c" }, new String[] { "fffaaa", "fffccc" });
////		// 删除记录
////		deleteRow("t_table", "row-0001");
////		// 删除表
////		dropTable("t_table");
//	}
}