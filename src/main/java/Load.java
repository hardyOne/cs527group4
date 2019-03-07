import java.util.List;

public class Load {

    public static void main(String[] args) throws Exception {
        Mysql mysql = new Mysql(Config.SERVER_NAME, Config.PORT_NUMBER, Config.USERNAME, Config.PASSWORD);
        mysql.createTables();
        for (String key : Config.KEYS) {
            GetObject getObject = new GetObject(key);
            List<List<String>> data = getObject.getContentOfObject();
            mysql.insert(key.split("\\.")[0], data);
        }
    }

}
