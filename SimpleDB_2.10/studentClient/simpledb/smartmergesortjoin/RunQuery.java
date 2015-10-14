package smartmergesortjoin;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import simpledb.remote.SimpleDriver;

public class RunQuery {

	public static void main(String[] args) {
		Connection conn = null;
		Driver d = new SimpleDriver();
		// you may change host if your SimpleDB server is running on a different
		// machine
		String host = "localhost";
		String url = "jdbc:simpledb://" + host;
		Statement s = null;
		long time0, time1;
		String query;
		ResultSet rs;
		try {
			conn = d.connect(url, null);
			s = conn.createStatement();

			// Run a join query
			query = "select a1, a2, a3 from test1, test2 where a1 = a3";
			System.out.println(query + "\n");
			time0 = System.currentTimeMillis();
			rs = s.executeQuery(query);
			while (rs.next()) {
				System.out.println("a1 : " + rs.getInt("a1") + " a3 : "
						+ rs.getInt("a3") + " a2 : " + rs.getInt("a2"));
			}
			time1 = System.currentTimeMillis();
			System.out.println("This query took: " + (time1 - time0) + " ms\n");

			// Run the same query again, this time it should be sorted and take
			// less time.
			query = "select a1, a2, a3 from test1, test2 where a1 = a3";
			System.out.println(query + "\n");
			time0 = System.currentTimeMillis();
			rs = s.executeQuery(query);
			while (rs.next()) {
				System.out.println("a1 : " + rs.getInt("a1") + " a3 : "
						+ rs.getInt("a3") + " a2 : " + rs.getInt("a2"));
			}
			time1 = System.currentTimeMillis();
			System.out.println("This query took: " + (time1 - time0) + " ms\n");

			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}