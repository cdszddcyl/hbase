package com.hbase.main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * @author StoneCai 2016年06月28日15:30:50 添加 用于操作数据库信息
 */
@SuppressWarnings({ "resource", "deprecation" })
public class HBaseOperation {

	static Configuration cfg = HBaseConfiguration.create();

	/**
	 * StoneCai 2016年06月28日15:31:32 添加 操作数据库
	 */
	public boolean createTabel(String tableName, String[] columns) {
		sysout("开始创建数据库");
		try {
			HBaseAdmin hBaseAdmin = new HBaseAdmin(cfg);
			if (hBaseAdmin.tableExists(tableName)) {// 校验表是否存在
				// 关闭表
				hBaseAdmin.disableTable(tableName);
				// 删除表
				hBaseAdmin.deleteTable(tableName);
				sysout("删除表：" + tableName);
			}

			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			for (String column : columns) {
				tableDescriptor.addFamily(new HColumnDescriptor(column));
			}
			hBaseAdmin.createTable(tableDescriptor);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("创建数据库结束");
		}
		return false;
	}

	/**
	 * Stone.Cai 2016年06月28日16:01:04 添加 插入数据
	 */
	public boolean insertData(String rowKey, String tableName, Map<String, Object> map) {
		sysout("插入数据开始");
		try {
			Put put = new Put(Bytes.toBytes(rowKey));
			// 获取需要操作的表
			HTable table = new HTable(cfg, Bytes.toBytes(tableName));
			// 获取所有的列族
			HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();
			for (HColumnDescriptor hColumnDescriptor : columnFamilies) {
				put.add(Bytes.toBytes(hColumnDescriptor.getNameAsString()),
						Bytes.toBytes(hColumnDescriptor.getNameAsString()),
						Bytes.toBytes(map.get(hColumnDescriptor.getNameAsString()).toString()));
			}
			table.put(put);
			sysout("插入数据成功");
		} catch (Exception e) {
			e.printStackTrace();
			sysout("插入数据失败");
		}
		return false;
	}

	/**
	 * Stone.Cai 2016年06月29日11:01:31 添加 根据家族插入数据
	 * 
	 * @param rowKey
	 * @param tableName
	 * @param map
	 * @return
	 */
	public boolean insertData(String rowKey, String tableName, String familyName, Map<String, Object> map) {
		sysout("插入数据开始");
		try {
			Put put = new Put(Bytes.toBytes(rowKey));
			// 获取需要操作的表

			HTable table = new HTable(cfg, Bytes.toBytes(tableName));
			// 获取所有的列族
			// HColumnDescriptor[] columnFamilies =
			// table.getTableDescriptor().getColumnFamilies();
			for (String column : map.keySet()) {
				put.add(Bytes.toBytes(familyName), Bytes.toBytes(column), Bytes.toBytes(map.get(column).toString()));
			}
			table.put(put);
			sysout("插入数据成功");
		} catch (Exception e) {
			e.printStackTrace();
			sysout("插入数据失败");
		}
		return false;
	}

	/**
	 * Stone.Cai 2016年06月28日16:29:56 添加 根据key获取数据
	 */
	public Map<String, Object> getResultByKey(String tableName, String rowKey) {
		Map<String, Object> map = new HashMap<String, Object>();
		sysout("开始查询");
		Result result = null;
		try {
			Get get = new Get(Bytes.toBytes(rowKey));
			HTable table = new HTable(cfg, Bytes.toBytes(tableName));// 获取表
			result = table.get(get);
			for (KeyValue kv : result.list()) {
				map.put(Bytes.toString(kv.getQualifier()), Bytes.toString(kv.getValue()));
				map.put("row_id", Bytes.toString(kv.getRow()));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return map;
	}

	/**
	 * Stone.Cai 2016年06月28日17:19:31 添加 查询数据数据集合
	 */
	public List<Map<String, Object>> getResultScann(String tableName) {
		Scan scan = new Scan();
		ResultScanner rs = null;
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try {
			HTable table = new HTable(cfg, Bytes.toBytes(tableName));
			rs = table.getScanner(scan);
			for (Result r : rs) {
				Map<String, Object> map = new HashMap<String, Object>();
				for (KeyValue kv : r.list()) {
					map.put(Bytes.toString(kv.getQualifier()), Bytes.toString(kv.getValue()));
					map.put("row_id", Bytes.toString(kv.getRow()));
				}
				list.add(map);
			}
			sysout("查询成功");
		} catch (Exception e) {
			sysout("查询失败");
			e.printStackTrace();
		} finally {
			rs.close();
		}
		return list;
	}

	/**
	 * Stone.Cai 2016年06月28日17:34:57 添加 分页
	 */
	public List<Map<String, Object>> getResultScann(String tableName, String start_rowkey, String stop_rowkey) {
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(start_rowkey));
		scan.setStopRow(Bytes.toBytes(stop_rowkey));
		ResultScanner rs = null;
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try {
			HTable table = new HTable(cfg, Bytes.toBytes(tableName));
			rs = table.getScanner(scan);
			for (Result r : rs) {
				Map<String, Object> map = new HashMap<String, Object>();
				for (KeyValue kv : r.list()) {
					map.put(Bytes.toString(kv.getQualifier()), Bytes.toString(kv.getValue()));
					map.put("row_id", Bytes.toString(kv.getRow()));
				}
				list.add(map);
			}
			sysout("查询成功");
		} catch (Exception e) {
			sysout("查询失败");
			e.printStackTrace();
		} finally {
			rs.close();
		}
		return list;
	}

	/**
	 * Stone.Cai 2016年06月28日17:52:23 添加 查询列数
	 */
	public List<Map<String, Object>> getResultByColumn(String tableName, String rowKey, String[] columnNames) {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try {
			HTable table = new HTable(cfg, Bytes.toBytes(tableName));
			Get get = new Get(Bytes.toBytes(rowKey));
			for (String column : columnNames) {
				get.addColumn(Bytes.toBytes(column), Bytes.toBytes(column));
			}
			Result result = table.get(get);
			Map<String, Object> map = new HashMap<String, Object>();
			for (KeyValue kv : result.list()) {
				map.put(Bytes.toString(kv.getQualifier()), Bytes.toString(kv.getValue()));
				map.put("row_id", Bytes.toString(kv.getRow()));
			}
			list.add(map);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;

	}

	/**
	 * Stone.Cai 2016年06月29日09:09:02 添加 更新列数据
	 */
	public boolean updateDataColumn(String tableName, String rowKey, String columnName, String value) {
		sysout("更新开始");
		try {
			HTable table = new HTable(cfg, Bytes.toBytes(tableName));
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(columnName), Bytes.toBytes(columnName), Bytes.toBytes(value));
			table.put(put);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			sysout("更新失败");
		} finally {
			sysout("更新完成");
		}
		return false;
	}

	/**
	 * Stone.Cai 2016年06月29日09:27:32 添加 删除指定列
	 */
	public boolean deleteColumn(String tableName, String rowKey, String columnName) {
		sysout("删除指定列开始");
		try {
			HTable table = new HTable(cfg, Bytes.toBytes(tableName));
			Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
			deleteColumn.deleteColumns(Bytes.toBytes(columnName), Bytes.toBytes(columnName));
			table.delete(deleteColumn);
			sysout("删除列成功");
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			sysout("删除列完成");
		}
		return false;
	}

	/**
	 * Stone.Cai 2016年06月29日09:35:24 添加 删除一行
	 */
	public boolean deleteRow(String tableName, String rowKey) {
		sysout("删除一行数据开始");
		try {
			HTable table = new HTable(cfg, Bytes.toBytes(tableName));
			Delete deleteRow = new Delete(Bytes.toBytes(rowKey));
			table.delete(deleteRow);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			sysout("删除失败");
		} finally {
			sysout("删除一行数据结束");
		}

		return false;
	}

	/**
	 * Stone.Cai 2016年06月29日09:48:31 添加 删除表
	 */
	public boolean deleteTable(String tableName) {
		sysout("删除表开始");
		try {
			HBaseAdmin admin = new HBaseAdmin(cfg);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			sysout("删除表成功");
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			sysout("删除表失败");
		} finally {
			sysout("删除表完成");
		}
		return false;
	}

	/**
	 * Stone.Cai 2016年06月29日17:21:32 添加 带条件查询列包含某些字符
	 */
	public List<Map<String, Object>> getResultByFilterColumn(String tableName, String family, String column,
			String filter) {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try {
			HTable table = new HTable(cfg, Bytes.toBytes(tableName));
			List<Filter> filters = new ArrayList<Filter>();
			Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(column), CompareOp.EQUAL,
					new SubstringComparator(filter));
			filters.add(filter1);
			FilterList filterList1 = new FilterList(filters);
			Scan scan = new Scan();
			scan.setFilter(filterList1);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				Map<String, Object> map = new HashMap<String, Object>();
				for (KeyValue kv : r.list()) {
					map.put(Bytes.toString(kv.getQualifier()), Bytes.toString(kv.getValue()));
					map.put("row_id", Bytes.toString(kv.getRow()));
				}
				list.add(map);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	/**
	 * Stone.Cai 2016年06月29日17:52:30 添加 rowkey含字符集
	 */
	public List<Map<String, Object>> getResultByFilterRow(String tableName, String filter) {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try {
			HTable table = new HTable(cfg, Bytes.toBytes(tableName));
			List<Filter> filters = new ArrayList<Filter>();
			Filter filter1 = new RowFilter(CompareOp.EQUAL, new SubstringComparator(filter));
			filters.add(filter1);
			FilterList filterList1 = new FilterList(filters);
			Scan scan = new Scan();
			scan.setFilter(filterList1);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				Map<String, Object> map = new HashMap<String, Object>();
				for (KeyValue kv : r.list()) {
					map.put(Bytes.toString(kv.getQualifier()), Bytes.toString(kv.getValue()));
					map.put("row_id", Bytes.toString(kv.getRow()));
				}
				list.add(map);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	/**
	 * Stone.Cai
	 * 2016年06月29日18:02:26
	 * 添加
	 * 根据列过滤
	 */
	public List<Map<String, Object>> getResultByFilterColumnAll(String tableName, String filter) {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try {
			HTable table = new HTable(cfg, Bytes.toBytes(tableName));
			List<Filter> filters = new ArrayList<Filter>();
			Filter filter1 = new ValueFilter(CompareOp.EQUAL, new SubstringComparator(filter));
			filters.add(filter1);
			FilterList filterList1 = new FilterList(filters);
			Scan scan = new Scan();
			scan.setFilter(filterList1);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				Map<String, Object> map = new HashMap<String, Object>();
				for (KeyValue kv : r.list()) {
					map.put(Bytes.toString(kv.getQualifier()), Bytes.toString(kv.getValue()));
					map.put("row_id", Bytes.toString(kv.getRow()));
				}
				list.add(map);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	/**
	 * Stoen.Cai 2016年06月28日15:39:20 添加 控制台打印信息
	 */
	public void sysout(String msg) {
		System.out.println(msg);
	}

}
