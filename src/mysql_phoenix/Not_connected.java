package mysql_phoenix;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;


public class Not_connected {
	public Connection connect = null;
	  public Statement statement = null;
	  public Statement statement1 = null;
	  public ResultSet resultSet = null;
	 public void readDataBase(ArrayList<String> relation) throws Exception {
		    try {
		      
		      Class.forName("com.mysql.jdbc.Driver");
		     
		      connect = DriverManager.getConnection("jdbc:mysql://localhost/bpi_2014?"
		              + "user=root&password=root");

		      statement = connect.createStatement();
		      
		      statement.executeUpdate("CREATE TABLE not_connected(eventA varchar(50),eventB varchar(50))");
          for(String s:relation)
          {
        	  if(s.contains("#"))
        	  {
        	  statement1 = connect.createStatement();
        	  //String progress=null,call=null,assigned=null,await=null,wait=null,user=null,vendor=null,implement=null,resolved=null,customer=null,cancelled=null,closed=null,unmatched=null;
        	  String g[]=s.split("#"); 
        	  statement1.executeUpdate("INSERT INTO not_connected(eventA,eventB) VALUES ('"+g[0]+"','"+g[1]+"')");
            statement1.close();
          }
          }
		    } catch (Exception e) {
		      
		    	  throw e;
		      
		    } 
		    finally {
		      connect.close();
		      statement.close();

		    }

		  }

}
