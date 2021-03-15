import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

class HiveJdbcClient {
  val driverName: String = "org.apache.hive.jdbc.HiveDriver";

  /** @param args
    * @throws SQLException
    */
  object Main extends App {
    try {
      Class.forName(driverName);
    } catch {
      case e: ClassNotFoundException => {
        e.printStackTrace();
        System.exit(1);
      }
    }

    //replace "hive" here with the name of the user the queries should run as
    val con: Connection = DriverManager.getConnection(
      "jdbc:hive2://localhost:10000/default",
      "hive",
      ""
    );
    val stmt: Statement = con.createStatement();
    val tableName: String = "testHiveDriverTable";
    stmt.execute("drop table if exists " + tableName);
    stmt.execute("create table " + tableName + " (key int, value string)");
    // show tables
    var sql: String = "show tables '" + tableName + "'";
    System.out.println("Running: " + sql);
    var res: ResultSet = stmt.executeQuery(sql);
    if (res.next()) {
      System.out.println(res.getString(1));
    }
    // describe table
    sql = "describe " + tableName;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1) + "\t" + res.getString(2));
    }

    // load data into table
    // NOTE: filepath has to be local to the hive server
    // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
    val filepath: String = "/tmp/a.txt";
    sql = "load data local inpath '" + filepath + "' into table " + tableName;
    System.out.println("Running: " + sql);
    stmt.execute(sql);

    // select * query
    sql = "select * from " + tableName;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(
        String.valueOf(res.getInt(1)) + "\t" + res.getString(2)
      );
    }

    // regular hive query
    sql = "select count(1) from " + tableName;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1));
    }
  }
}
