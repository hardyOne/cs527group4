import java.sql.*;
import java.util.List;

public class Mysql {

    private String serverName;
    private String portNumber;
    private String userName;
    private String password;
    private Connection conn;
    private Statement statement;
    private String databaseName;
    private String[] tableNames;

    public Mysql(String serverName, String portNumber, String userName, String password) throws Exception {
        this.serverName = serverName;
        this.portNumber = portNumber;
        this.userName = userName;
        this.password = password;
        tableNames = Config.TABLE_NAMES;
        getConnection();
    }

    public static void main(String[] args) throws Exception{
        String serverName = Config.SERVER_NAME;
        String portNumber = Config.PORT_NUMBER;
        String userName = Config.USERNAME;
        String password = Config.PASSWORD;
        Mysql mysql = new Mysql(serverName, portNumber, userName, password);
        mysql.getConnection();
        mysql.createTables();

    }

    public Connection getConnection() throws Exception {
        String connStr = "jdbc:mysql://"+this.serverName+":"+this.portNumber+"/nwind?user="+this.userName+"&password="+this.password;
        conn = DriverManager.getConnection(connStr);
        System.out.println("Connected to database");
        statement = conn.createStatement();
        return conn;
    }

    public void cteateDB(String name) throws Exception {
        databaseName = name;
        String sql = "CREATE DATABASE nwind;";
        statement.execute(sql);
    }
    public void createTables() throws Exception{
        for (String tableName: tableNames) {
            try {
                String sql = "drop table " + tableName + ";";
                statement.execute(sql);
                System.out.println("drop table " + tableName + " successfully!");
            } catch (Exception e) {
                // do nothing
            }
        }


        // Categories
        String sql = "create table Categories (" +
                "CategoryID int NOT NULL PRIMARY KEY, " +
                "CategoryName varchar(255) NOT NULL, " +
                "Description varchar(255) NOT NULL " +
                ");";
        statement.execute(sql);

        // Customers
        sql = "create table Custormers (" +
                "CustomerID varchar(255) not null primary key, " +
                "CompanyName varchar(255) not null, " +
                "ContactName varchar(255) not null, " +
                "ContactTitle varchar(255) not null, " +
                "Address varchar(255) not null, " +
                "City varchar(255) not null, " +
                "Region varchar(255), " +
                "PostcalCode varchar(255), " +
                "Country varchar(255) not null, " +
                "Phone varchar(255) not null, " +
                "Fax varchar(255) not null " +
                ");";
        statement.execute(sql);

        // Employees
        sql = "create table Employees ( " +
                "EmployeeID int not null primary key, " +
                "LastName varchar(255) not null, " +
                "FirstNamne varchar(255) not null, " +
                "Title varchar(255) not null, " +
                "TitleOfCourtesy varchar(255) not null, " +
                "BirthDate varchar(255) not null, " +
                "HireDate varchar(255) not null, " +
                "Address varchar(255) not null, " +
                "City varchar(255) not null, " +
                "Region varchar(255) not null, " +
                "PostalCode varchar(255) not null, " +
                "Country varchar(255) not null, " +
                "HomePhone varchar(255) not null, " +
                "Extension varchar(255) not null, " +
                "Notes varchar(255) not null, " +
                "ReportsTo int " +
                ");";
        statement.execute(sql);

        // Order_Details
        sql = "create table Order_Details ( " +
                "OrderID int not null, " +
                "ProductID int not null, " +
                "UnitPrice decimal(38,2) not null, " +
                "Quantity int not null, " +
                "Discount decimal(38,2) not null " +
                ");";
        statement.execute(sql);

        // Orders
        sql = "create table Orders ( " +
                "OrderID int not null primary key, " +
                "CustomerID varchar(255) not null, " +
                "EmployeeID int not null, " +
                "OrderDate varchar(255) not null, " +
                "RequireDate varchar(255) not null, " +
                "ShippedDate varchar(255) not null, " +
                "ShipVia int not null, " +
                "Freight decimal(38,2) not null, " +
                "ShipName varchar(255) not null, " +
                "ShipAddress varchar(255) not null, " +
                "ShipCity varchar(255) not null, " +
                "ShipRegion varchar(255), " +
                "ShipPostalCode varchar(255) not null, " +
                "ShipCountry varchar(255) " +
                ");";
        statement.execute(sql);

        // Products
        sql = "create table Products ( " +
                "ProductID int not null primary key, " +
                "ProductName varchar(255) not null, " +
                "SupplierID int not null, " +
                "CategoryID int not null, " +
                "QuantityPerUnit varchar(255) not null, " +
                "UnitPrice decimal(38, 2) not null, " +
                "UnitsInStock varchar(255) not null, " +
                "UnitsOnOrder int not null, " +
                "ReorderLevel int not null, " +
                "Discontinued int not null " +
                ");";
        statement.execute(sql);

        // Shippers
        sql = "create table Shippers ( " +
                "ShipperID int not null primary key, " +
                "CompanyName varchar(255) not null, " +
                "Phone varchar(255) not null " +
                ");";

        statement.execute(sql);

        // Supplier
        sql = "create table Suppliers ( " +
                "SupplierID int not null, " +
                "CompanyName varchar(255) not null, " +
                "ContactName varchar(255) not null, " +
                "ContactTitle varchar(255) not null, " +
                "Address varchar(255) not null, " +
                "City varchar(255) not null, " +
                "Region varchar(255), " +
                "PostalCode varchar(255) not null, " +
                "Country varchar(255) not null, " +
                "Phone varchar(255) not null, " +
                "Fax varchar(255), " +
                "HopePage varchar(255) " +
                ");";
        statement.execute(sql);

    }

    public void dropTable(String tableName) {
        try {
            String sql = "drop table " + tableName + ";";
            statement.execute(sql);
            System.out.println("drop table " + tableName + " successfully!");
        } catch (Exception e) {

        }
    }

    public void createCustormers() throws Exception{
        // Customers
        String sql = "create table Custormers (" +
                "CustomerID varchar(255) not null primary key, " +
                "CompanyName varchar(255) not null, " +
                "ContactName varchar(255) not null, " +
                "ContactTitle varchar(255) not null, " +
                "Address varchar(255) not null, " +
                "City varchar(255) not null, " +
                "Region varchar(255), " +
                "PostcalCode varchar(255), " +
                "Country varchar(255) not null, " +
                "Phone varchar(255) not null, " +
                "Fax varchar(255) not null " +
                ");";
        statement.execute(sql);
    }

    // insert one row of data(include all fields)
    public void insert(String tableName, List<List<String>> data) throws Exception{
        Statement statement;
        String base = "insert into " + tableName + " values ";
        // first row is header
        for (int i = 1; i < data.size(); i++) {
            StringBuilder sb = new StringBuilder(base);
            sb.append("(");
            for (int j = 0; j < data.get(i).size(); j++) {
                if (j == 0) {
                    sb.append("\""+data.get(i).get(j)+"\"");
                } else {
                    sb.append(", \"" + data.get(i).get(j) + "\"");
                }
            }
            sb.append(");");
            statement = conn.createStatement();
            System.out.println(sb.toString());
            statement.execute(sb.toString());
        }
    }




}
