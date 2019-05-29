/**
 * 
 */
package demo;

import java.sql.*;
import demo.arrayjson;
import org.json.JSONObject;
import org.json.simple.JSONArray;
import java.util.Properties;
import java.util.Timer;

/**
 * @author Vikram
 *
 */

public class arrayjson {

	public static void main(String[] args) {
		long beforeUsedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

		long start = System.currentTimeMillis();

		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/newdb", "root", "");

			Statement st1 = con.createStatement();
			// st1.setFetchSize(1000);
			ResultSet rs1 = st1.executeQuery("SELECT * FROM account");
			JSONObject json = new JSONObject();
			ResultSetMetaData rsmd = rs1.getMetaData();
			JSONObject jo = new JSONObject();

			while (rs1.next()) {
				json.put("accountid", rs1.getInt("accountid"));
				json.put("name", rs1.getString("name"));
				json.put("updated", rs1.getString("updated"));

				Statement st2 = con.createStatement();
				// st2.setFetchSize(1000);
				ResultSet rs2 = st2.executeQuery("SELECT * FROM contact where accountid = " + rs1.getInt("accountid"));

				ResultSetMetaData rsmd1 = rs2.getMetaData();
				JSONArray json1 = new JSONArray();
				while (rs2.next()) {
					int numColumns = rsmd1.getColumnCount();
					JSONObject obj = new JSONObject();
					for (int i = 1; i <= numColumns; i++) {
						String column_name = rsmd1.getColumnName(i);
						obj.put(column_name, rs2.getObject(column_name));
					}

					json1.add(obj);

					Statement st3 = con.createStatement();
					// st3.setFetchSize(1000);
					JSONArray ja = new JSONArray();
					ResultSet rs3 = st3
							.executeQuery("SELECT * FROM address where accountid = " + rs2.getInt("accountid"));
					ResultSetMetaData rsmd2 = rs3.getMetaData();
					while (rs3.next()) {
						int numColumns1 = rsmd2.getColumnCount();
						JSONObject obj1 = new JSONObject();
						for (int a = 1; a <= numColumns1; a++) {
							String colum_name = rsmd2.getColumnName(a);
							obj1.put(colum_name, rs3.getObject(colum_name));
						}
						ja.add(obj1);

					}
					obj.put("address", ja);
				}
				json.put("contact", json1);

				rs2.close();
				System.out.println(json.toString());

			}

			long end = System.currentTimeMillis();
			long duration = end - start;
			long afterUsedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
			long actualMemUsed = afterUsedMem - beforeUsedMem;
			System.out.println("Total memory used to complete task is - " + actualMemUsed);

			System.out.println("Total time taken to complete task is - " + duration + " - milliseconds");
			System.out.println("Total time taken to complete task is - " + duration / 1000 + " - seconds");

			rs1.close();
			con.close();
		} catch (

		Exception e) {
			e.printStackTrace();
		}
	}
}
