package com.EmployeePayroll_JDBC;

import java.sql.*;

public class JDBCConnection 
{
    private static final String URL = "jdbc:mysql://localhost:3306/payroll_service?useSSL=false";
	private static final String USER = "root";
	private static final String PASSWORD = "13041997@Mda";

	public static void main( String[] args )
    {
    	Connection connection;
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			System.out.println("Driver loaded!");
			connection = DriverManager.getConnection(URL, USER, PASSWORD);
			System.out.println(connection + " Connection established!");
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException("Cannot find the driver in the classpath", e);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
    
}