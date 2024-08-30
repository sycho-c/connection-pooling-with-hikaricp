package com.connection.pooling;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class DatabaseConnectionTest {

    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/articles";
    private static final String USER = "username";
    private static final String PASSWORD = "password";
    private static final String SQL_QUERY = "SELECT * FROM AUTHORS WHERE FIRST_NAME = 'Adam'";

    public static void main(String[] args) {
        int requestCount = 100;

        System.out.println("Running tests without connection pool:");
        runWithoutConnectionPool(requestCount);

        System.out.println("\nRunning tests with HikariCP:");
        runWithHikariCP(requestCount);
    }

    private static void runWithoutConnectionPool(int requestCount) {
        try {
            long totalConnectionTime = 0;
            long totalQueryTime = 0;

            for (int i = 0; i < requestCount; i++) {
                long startConnectionTime = System.nanoTime();
                Connection connection = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
                long endConnectionTime = System.nanoTime();
                totalConnectionTime += (endConnectionTime - startConnectionTime);

                Statement statement = connection.createStatement();

                long startQueryTime = System.nanoTime();
                ResultSet resultSet = statement.executeQuery(SQL_QUERY);
                while (resultSet.next()) {
                    String name = resultSet.getString("LAST_NAME");
                }
                long endQueryTime = System.nanoTime();
                totalQueryTime += (endQueryTime - startQueryTime);

                connection.close();
            }

            System.out.printf("Average connection time: %.2f ms\n", totalConnectionTime / 1_000_000.0 / requestCount);
            System.out.printf("Average query time: %.2f ms\n", totalQueryTime / 1_000_000.0 / requestCount);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void runWithHikariCP(int requestCount) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(JDBC_URL);
        hikariConfig.setUsername(USER);
        hikariConfig.setPassword(PASSWORD);
        hikariConfig.setDriverClassName("com.mysql.cj.jdbc.Driver");

        try (HikariDataSource dataSource = new HikariDataSource(hikariConfig)) {
            long totalConnectionTime = 0;
            long totalQueryTime = 0;

            for (int i = 0; i < requestCount; i++) {
                long startConnectionTime = System.nanoTime();
                Connection connection = dataSource.getConnection();
                long endConnectionTime = System.nanoTime();
                totalConnectionTime += (endConnectionTime - startConnectionTime);

                Statement statement = connection.createStatement();

                long startQueryTime = System.nanoTime();
                ResultSet resultSet = statement.executeQuery(SQL_QUERY);
                while (resultSet.next()) {
                    String name = resultSet.getString("LAST_NAME");
                }
                long endQueryTime = System.nanoTime();
                totalQueryTime += (endQueryTime - startQueryTime);

                connection.close();
            }

            System.out.printf("Average connection time: %.2f ms\n", totalConnectionTime / 1_000_000.0 / requestCount);
            System.out.printf("Average query time: %.2f ms\n", totalQueryTime / 1_000_000.0 / requestCount);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
