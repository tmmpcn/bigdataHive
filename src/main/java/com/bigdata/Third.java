package com.bigdata;

import java.sql.*;

public class Third {

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
        String sql = "select collect_set(m.MovieName)[0] as MovieName,avg(t.rate) as avgrate from (select MovieID from t_rating where UserID in(select UserID from(select collect_set(a.UserID)[0] as UserID,count(*) as num from t_rating a left join t_user b on a.UserID=b.UserID where b.Sex='F' group by a.UserID order by num desc limit 1) t1) order by rate desc limit 10) t2 join t_rating t on t.MovieID=t2.MovieID left join t_movie m on m.MovieID=t2.MovieID group by t2.MovieID";//
        //执行预编译执行对象PreparedStatemen,只接受值类型参数
        PreparedStatement preparedStatement = con.prepareStatement(sql);
        //返回结果集
        ResultSet rst = preparedStatement.executeQuery();
        //ResultSet 是一根指向某数据表的指针，读取数据是一行一行从数据表中读取的
        while (rst.next()){
            System.out.println(rst.getString("MovieName")+"\t"+rst.getString("avgrate"));
        }
        //关闭连接
        con.close();

    }
    
}
