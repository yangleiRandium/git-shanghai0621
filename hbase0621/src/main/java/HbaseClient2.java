import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HbaseClient2 {
    public static void main(String[] args) {

    }
    public static boolean isTableExist(String tableName) throws IOException {
        //1.创建配置信息并配置
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hadoop105,hadoop106,hadoop107");
        //2.获取与Hbase的连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        //3.获取DDL操作对象
        Admin admin = connection.getAdmin();


        return false;
    }
}
