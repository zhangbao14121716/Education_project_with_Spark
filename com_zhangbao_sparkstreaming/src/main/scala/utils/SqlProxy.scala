package utils

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.alibaba.druid.pool.DruidDataSource

/**
 * @Author: Zhangbao
 * @Date: 16:35 2020/9/12
 * @Description:
 *  数据库操作工具类
 *  包含：修改操作
 *       查询操作：或者对查询到结果集进行处理
 */


trait QueryCallback {
  //处理从数据库查询到的结果集
  def process(rs: ResultSet)
}

//Sql代理--更新，查询语句的sql操作工具类
class SqlProxy {
  //定义结果集用于存放查询结果集
  private var rs: ResultSet = _
  //定义预执行语句
  var psmt: PreparedStatement = _

  /**
   * 执行修改语句
   * 不能使用select * 引文返回的是一个结果集，返回值是int类型接收的
   * @param conn 数据库连接
   * @param sql sql语句
   * @param params 占位符
   * @return 返回 （1）DML操作语句的行数
   *             （2）不返回任何内容的SQL语句为0
   */
  def executeUpdate(conn: Connection, sql: String, params: Array[Any]): Int ={
    var result = 0

    try {
      //预编译语句参数设置
      psmt = conn.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          //对每个占位符，设置param
          psmt.setObject(i + 1, params(i))
        }
      }
      //预执行
      //注意！这里不是递归
      result = psmt.executeUpdate()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result
  }

  /**
   * 执行查询语句
   *
   * @param conn 数据库连接
   * @param sql sql查询语句
   * @param params 参数
   * @return
   */
  def executeQuery(conn: Connection, sql:String, params:Array[Any], queryCallback: QueryCallback):Unit ={
    rs = null
    try {
      //预编译语句参数设置
      psmt = conn.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          psmt.setObject(i + 1, params(i))
        }
      }
      rs = psmt.executeQuery()
      //调用特质里的抽象方法，所以在用这个方法的时候需要实现一个匿名类
      queryCallback.process(rs)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
   * 关闭数据库连接
   *
   * @param conn 数据库连接
   */
  def shutdown(conn :Connection): Unit = {
    //德鲁伊资源管理工具方法
    DataSourceUtil.closeResource(rs, psmt, conn)
  }
}
