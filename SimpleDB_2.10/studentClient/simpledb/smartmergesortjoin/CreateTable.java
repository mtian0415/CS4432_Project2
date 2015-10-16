package smartmergesortjoin;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import simpledb.remote.SimpleDriver;

/* CS4432-Project2(Task4,5): a test client created by Mi Tian, Yuchen Liu
* This class creates a table 
*/
public class CreateTable {

	final static int maxSize = 10000;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Connection conn = null;
		// you may change host if your SimpleDB server is running on a different
		// machine

		try {
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);
			Statement stmt = conn.createStatement();

			System.out.println("create table test1 (a1 int, a2 int, rName varchar(10))");
			stmt.executeUpdate("create table test1 (a1 int, a2 int)");

			System.out.println("create table test1 (a3 int, a4 int)");
			stmt.executeUpdate("create table test2 (a3 int, a4 int)");

			Random rand =  new Random(1);
			String query;
			System.out.println("insert into test1 (a1, a2, sName varchar(10))");
			for (int j = 0; j < maxSize; j++) {
				System.out.println("insert " + j);
				query = "insert into test1(a1, a2) values ("
						+ rand.nextInt(maxSize) + "," + rand.nextInt(maxSize) + ", 'void' )";
				stmt.executeUpdate(query);
			}

			rand = new Random(1);
			System.out.println("insert into test2 (a3, a4)");
			for (int j = 0; j < maxSize; j++) {
				query = "insert into test2(a3, a4) values ("
						+ rand.nextInt(maxSize) + "," + rand.nextInt(maxSize) + ", 'void' )";
				stmt.executeUpdate(query);
			}

			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}