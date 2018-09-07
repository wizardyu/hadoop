package people.bbs.hadoop.hbase;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Hello world!
 */
public class App {
	public static void main(String[] args) throws IOException {

		// Instantiating configuration class
		Configuration con = HBaseConfiguration.create();
		con.set("key", "value");
		Connection connection = ConnectionFactory.createConnection(con);
		Admin admin = connection.getAdmin();

		// creating table descriptor
		TableDescriptorBuilder.ModifyableTableDescriptor table = new TableDescriptorBuilder.ModifyableTableDescriptor(TableName.valueOf("testHbase2"));

		// creating column family descriptor
		ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor family = new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(toBytes("columnfamily2"));

		// adding coloumn family to HTable
		table.setColumnFamily(family);

		// 创建表
		admin.createTable(table);

		// 获取所有表名
		TableName[] tableNames = admin.listTableNames();
		TableName tb = tableNames[0];
		// 判断tb是否启用
		// 禁用表tb
		if (admin.isTableEnabled(tb)) {
			admin.disableTable(tb);
		}
		// 启用表
		if (!admin.isTableDisabled(tb)) {
			admin.enableTable(tb);
		}

		// 列信息
		ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor coums = new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(toBytes("column12"));

		// 指定某列最大版本号
		coums.setMaxVersions(9);
		// 指定某列当前版本及最大版本
		coums.setVersions(1, 9);
		// 添加列簇
		admin.addColumnFamily(tb, coums);

		// 判断表是否存在
		if (admin.tableExists(tb)) {
			// 删除表
			admin.deleteTable(tb);
		}
		for (TableName tableName : tableNames) {
			System.out.println(tableName);
		}

		// 客户端api:table
		Table htb = connection.getTable(tb);

		// 1 、新增列簇数据
		Put p = new Put(Bytes.toBytes("row1"));
		p.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes("raju"));

		p.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"), Bytes.toBytes("hyderabad"));

		p.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("designation"), Bytes.toBytes("manager"));

		p.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("salary"), Bytes.toBytes("50000"));
		htb.put(p);

		// 2、 更新列簇

		p.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"), Bytes.toBytes("Delih"));
		htb.put(p);

		// 3、 获取数据
		Get g = new Get(toBytes("row1"));
		// 获取指定列簇下所有列
		g.addFamily(Bytes.toBytes("professional"));
		// 获取指定列簇下指定列
		g.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("salary"));

		// 获取结果集
		Result rs = htb.get(g);
		byte[] value = rs.getValue(Bytes.toBytes("personal"), Bytes.toBytes("name"));

		byte[] value1 = rs.getValue(Bytes.toBytes("personal"), Bytes.toBytes("city"));

		// 4、 删除数据

		Delete d = new Delete(toBytes("row1"));
		d.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("salary"));
		htb.delete(d);

		// 5、 扫描
		Scan scan = new Scan();
		// 扫描指定列
		scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"));

		// 扫描指定列簇
		scan.addFamily(Bytes.toBytes("personal"));

		// 扫描结果集
		ResultScanner resultScanner = htb.getScanner(scan);

		// 迭代结果
		resultScanner.iterator();

		// 关闭Htable
		htb.close();

		// 停止hbase
		admin.shutdown();

	}
}