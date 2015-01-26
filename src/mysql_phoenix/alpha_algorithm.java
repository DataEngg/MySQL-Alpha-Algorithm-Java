package mysql_phoenix;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
public class alpha_algorithm {
	
	  public Connection connect = null;
	  public Statement statement = null;
	  public ResultSet resultSet = null;
	  public static HashSet<String> substatus=new HashSet<String>();
	  public static HashMap<String,ArrayList<String>> sequence=new HashMap<String,ArrayList<String>>();
	  
	  
	  
	  
	  public void readDataBase() throws Exception {
		    try {
		      
		      Class.forName("com.mysql.jdbc.Driver");
		     
		      connect = DriverManager.getConnection("jdbc:mysql://localhost/bpi_2014?"
		              + "user=root&password=root");

		
		      
		     
		      
		      Statement st=connect.createStatement();
		      //statement.executeUpdate("CREATE TABLE initial(initial varchar(50))");
		      //statement.executeUpdate("CREATE TABLE final(final varchar(50))");
		      resultSet = st.executeQuery("select DISTINCT caseid from bpi_2014.eventlog");
		      initial_final_events(resultSet);
		    } catch (Exception e) {
		      
		    	  throw e;
		      
		    } 
		    

		  }

	  
	  
	  public void initial_final_events(ResultSet resultset) throws SQLException{
		  ResultSet result=null;
		  Statement st=connect.createStatement();
		  int i=1;
		  while(resultset.next())
		  {
			  String caseid=resultset.getString("caseid");
			  System.out.println(resultset.getString("caseid"));
			  ArrayList<String> trace=new ArrayList<String>();
			  
			  result=st.executeQuery("Select activity from eventlog where caseid='"+caseid+"'");
			 System.out.println(i);
			  while(result.next())
			  {   
				  trace.add(result.getString("activity"));
				  
			  }
			  sequence.put(caseid,trace);
			  i++;
	}
		  
	  }
		  
	  
	  
	  
	  
	 
			  
		    
		    
		public static void main(String args[]) throws Exception
		  {
			alpha_algorithm a=new alpha_algorithm();
			
			Relation r=new Relation();
			
			try {
				System.out.println("Started alpha");
				a.readDataBase();
				System.out.println("Started relation");
				r.read_database(sequence);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		  }

}
