package com.hbase.main;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TestHbase {

	// private static HBaseConfiguration hbaseConfig=null;
	// static Configuration cfg = HBaseConfiguration.create();
	// static {
	// cfg.set("hbase.zookeeper.quorum", "10.211.55.34");
	// cfg.set("hbase.zookeeper.property.clientPort", "2181");
	// cfg.addResource("classpath:/hbase/hbase-site.xml");
	// }
	public static void main(String[] args) {
		HBaseOperation hbase = new HBaseOperation();
		// 创建表
		// hbase.createTabel("email_logs", new String[]{"email"});
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("email_id", "555565");
		map.put("email_log", "数据结构");
		map.put("email_name", "数据结果");
		// hbase.insertData(UUID.randomUUID().toString(),"email_logs","email",
		// map);
		// 根据id查询数据
		// Map<String, Object> map=
		// hbase.getResultByKey("email_logs","8bfcc13e-0462-46ce-b2e7-bf23ebfa7b39");
		// for (Object string : map.values()) {
		// System.out.println(string);
		// };
		// 查询所有数据
		// List<Map<String, Object>> list=
		// hbase.getResultScann("email_logs","42bea56f-db26-46e2-ab89-91454d5b7b96","b8078d0e-05d2-4255-832a-15bda9bb3fa5");
		// for (Map<String, Object> ma : list) {
		// System.out.println(ma.keySet().toString());
		// }
		// 查询根据KEY列数据
		// List<Map<String, Object>> list=
		// hbase.getResultByColumn("email_logs","42bea56f-db26-46e2-ab89-91454d5b7b96",new
		// String[]{"email_log","email_name"});
		// for (Map<String, Object> ma : list) {
		// System.out.println(ma.keySet().toString());
		// }
		// 根据KEY更新列数据
		// hbase.deleteColumn("email_logs",
		// "42bea56f-db26-46e2-ab89-91454d5b7b96", "email_name");
		// 删除一行数据
		// hbase.deleteRow("email_logs",
		// "42bea56f-db26-46e2-ab89-91454d5b7b96");
		// 删除表
		// hbase.deleteTable("email_logs");

		// hbase.creteTable("email_logs", new String[]{"cds","zdd"});

		// List<Map<String, Object>> list =
		// hbase.getResultByFilterColumn("email_logs", "email", "email_name",
		// "你看2555555");
		// for (Map<String, Object> ma : list) {
		// System.out.println(ma.values().toString());
		// }
		List<Map<String, Object>> list = hbase.getResultByFilterColumnAll("email_logs", "55");
		for (Map<String, Object> ma : list) {
			//System.out.println(ma.values().toString()+"====="+ma.keySet().toString());
			Map<String, Object> maps= hbase.getResultByKey("email_logs", ma.get("row_id").toString());
			System.out.println(maps.values().toString());
		}
	}

}
