public class Config {
    // s3
    public static String BUCKET_NAME = "cs527group4";
    public static String[] KEYS = {"Categories.csv", "Custormers.csv", "Employees.csv", "Order_Details.csv", "Orders.csv", "Products.csv", "Shippers.csv", "Suppliers.csv"};
    public static String REGION = "us-east-1";

    // database
    public static String SERVER_NAME = "cs527.c7ftzzwu1edi.us-east-1.rds.amazonaws.com";
    public static String PORT_NUMBER = "3306";
    public static String USERNAME = "cs527_group4";
    public static String PASSWORD = "bestfriend";
    public static String DATABASE_NAME = "nwind";

    // tables
    public static String[] TABLE_NAMES = {"Categories", "Custormers", "Employees", "Order_Details", "Orders", "Products", "Shippers", "Suppliers"};

}
