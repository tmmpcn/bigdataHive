package com.bigdata;

import java.sql.*;


/**
 * Hello world!
 *
 */
public class First 
{
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public static void main( String[] args ) throws SQLException
    {
        try {
            //加载驱动
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        //连接对象，连接hive的本机ip,不指定端口，默认端口号是10000，数据库名是mydemo，连接hive的账号是hive，密码是123456
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/bigdata");
        String sql = "select b.Age as Age,avg(a.Rate) as Rate from t_rating as a left join t_user as b on a.UserID=b.UserID where a.MovieID=2116 group by age";
        //执行预编译执行对象PreparedStatemen,只接受值类型参数
        PreparedStatement preparedStatement = con.prepareStatement(sql);
        //返回结果集
        ResultSet rst = preparedStatement.executeQuery();
        //ResultSet 是一根指向某数据表的指针，读取数据是一行一行从数据表中读取的
        while (rst.next()){
            System.out.println(rst.getString("Age")+"\t"+rst.getString("Rate"));
        }
        //关闭连接
        con.close();

    }
}
