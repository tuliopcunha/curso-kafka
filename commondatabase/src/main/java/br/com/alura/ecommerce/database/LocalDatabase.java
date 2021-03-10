package br.com.alura.ecommerce.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LocalDatabase {

    private final Connection connection;

    public LocalDatabase(String name) throws SQLException {
        String url = "jdbc:sqlite:"+ name + ".db";
        connection = DriverManager.getConnection(url);

    }

    // yes, this is wy too generic
    // according to yout database tool, avoid injection
    public void createIfNotExists(String sql){
        try {
            connection.createStatement().execute(sql);
        } catch(SQLException ex) {
            // be careful, the sql could be wrong, be reallllly careful
            ex.printStackTrace();
        }
    }

    public boolean update(String statement, String ... params) throws SQLException {
        PreparedStatement preparedStatement = prepare(statement, params);
        return preparedStatement.execute();
    }

    public ResultSet query(String statement, String ... params) throws SQLException {
        PreparedStatement preparedStatement = prepare(statement, params);
        return preparedStatement.executeQuery();
    }

    private PreparedStatement prepare(String statement, String[] params) throws SQLException {
        var preparedStatement = connection.prepareStatement(statement);
        for (int i = 0; i < params.length; i++) {
            preparedStatement.setString(i + 1, params[i]);
        }
        return preparedStatement;
    }

    public void close() throws SQLException {
        connection.close();
    }
}

