import java.sql.{Connection, ResultSet, Statement, DriverManager, SQLException}

/** @param args
  * @throws SQLException
  */
object HiveJdbcClient extends App {
  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  val connectionURL: String = "jdbc:hive2://localhost:10000/default"
  var con: Connection = null
  try {
    // load hive driver into classpath
    Class.forName(driverName)
    con = DriverManager.getConnection(connectionURL, "", "")

    // custom queries below
    createTable("testTable")
    showTables()
  } catch {
    case e: Exception => {
      e.printStackTrace()
      System.exit(1)
    }
  }
  con.close()

  // create hive sql statements
  def createTable(tableName: String) {
    val stmt: Statement = con.createStatement()
    val table = tableName
    // execute query
    stmt.execute("DROP TABLE IF EXISTS " + tableName)
    stmt.execute("CREATE TABLE " + tableName + " (KEY INT, VALUE STRING)")
  }

  // lists/displays all the base tables and views from the current db
  def showTables() {
    // var sql: String = "show tables '" + tableName + "'"
    val stmt: Statement = con.createStatement()
    var sql: String = "show tables"
    println("Running: " + sql)
    var res: ResultSet = stmt.executeQuery(sql)
    if (res.next()) {
      println(res.getString(1))
    }
  }

}
