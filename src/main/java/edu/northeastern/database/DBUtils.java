package edu.northeastern.database;


import java.sql.*;

/**
 * This class provides utility functions to connect to a MySQL database.
 */
public class DBUtils {


    private String url;
    private String user;
    private String password;
    private Connection con = null;

    public DBUtils(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.con = getConnection();
    }

    /**
     * Gets a connection to the MySQL database using the connector URL, database username, and database password.
     * @return connection
     */
    public Connection getConnection()
    {
        if (con == null) {
            try {
                con = DriverManager.getConnection(url, user, password);
                return con;
            } catch (SQLException e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }

        return con;
    }

    /**
     * Closes the connection to the database.
     */
    public void closeConnection() {
        try {
            con.close();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Executes a MySQL update statement to the database.
     * @param update MYSQL update statement
     */
    public void executeUpdate(String update)
    {
        try {

            // get connection and initialize statement
            Connection con = getConnection();
            Statement stmt = con.createStatement();

            stmt.executeUpdate(update);
            stmt.close();
        } catch (SQLException e) {
            System.err.println("ERROR: Could not execute update: "+ update);
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
    }

}
