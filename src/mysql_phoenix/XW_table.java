package mysql_phoenix;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class XW_table {

	public static HashMap<String, String> eventA = new HashMap<String, String>();
	public static HashMap<String, String> eventB = new HashMap<String, String>();
	//public static HashMap<String,String> compare = new HashMap<String,String>();
	public static ArrayList<String> compare = new ArrayList<String>();

	public static void main(String args[]) {
		Connection connect = null;
		Statement statement2 = null;
		ResultSet resultSet = null;
		ResultSet resultset2 = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
		     
		      connect = DriverManager.getConnection("jdbc:mysql://localhost/bpi_2014?"
		              + "user=root&password=root");
			//statement = connect.createStatement();
			statement2=connect.createStatement();
			
			long lStartTime = System.currentTimeMillis();
			
			resultset2 = statement2.executeQuery("SELECT * FROM not_connected");
			/*while (resultset2.next()) {
				if (resultset2.getString("EVENT").contains(";")) {
					compare.add(resultset2.getString("EVENT").trim());

				}

			}*/
			while (resultset2.next()) {
				
					compare.add(resultset2.getString("eventA")+";"+resultset2.getString("eventB"));
					compare.add(resultset2.getString("eventB")+";"+resultset2.getString("eventA"));
					}
			
              calling();
			
			long lEndTime = System.currentTimeMillis();
			long difference = lEndTime - lStartTime;
			 
			System.out.println("Elapsed milliseconds: " + difference);
			
		} catch (Exception e) {
			System.out.println(e);
		}

	}
	public static void calling()
	{
		Connection connect = null;
		Statement statement = null;
		Statement statement1 = null;
		Statement statement3 = null;
		Statement statement4 = null;
		Statement statement5 = null;
		Statement statement6 = null;
		ResultSet resultSet=null;
		try{
			Class.forName("com.mysql.jdbc.Driver");
		     
		      connect = DriverManager.getConnection("jdbc:mysql://localhost/bpi_2014?"
		              + "user=root&password=root");
			statement = connect.createStatement();
			statement1 = connect.createStatement();
			statement3= connect.createStatement();
			statement4 = connect.createStatement();
			statement5 = connect.createStatement();
			statement6 = connect.createStatement();
			
			statement1.executeUpdate("DROP TABLE IF EXISTS safeEventB");
			statement3.executeUpdate("DROP TABLE IF EXISTS safeEventA");
			resultSet = statement6.executeQuery("SELECT * FROM bpi_2014.casulaity");
			statement4
			.executeUpdate("CREATE TABLE safeEventA(setA varchar(200),setB varchar(200),PRIMARY KEY(setA,setB))");
			statement5
			.executeUpdate("CREATE TABLE safeEventB(setA varchar(200),setB varchar(200),PRIMARY KEY(setA,setB))");
		while (resultSet.next()) {
			if (eventA.containsKey(resultSet.getString("eventA"))) {
				String s = eventA.get(resultSet.getString("eventA"));
				StringBuilder build = new StringBuilder(s);
				build.append(";" + resultSet.getString("eventB"));
				eventA.put(resultSet.getString("eventA"), build.toString());

			} else {
				String s = resultSet.getString("eventB");
				eventA.put(resultSet.getString("eventA"), s);

			}
			if (eventB.containsKey(resultSet.getString("eventB"))) {
				String s = eventB.get(resultSet.getString("eventB"));
				StringBuilder build = new StringBuilder(s);
				build.append(";" + resultSet.getString("eventA"));
				eventB.put(resultSet.getString("eventB"), build.toString());
			} else {
				String s = resultSet.getString("eventA");
				eventB.put(resultSet.getString("eventB"), s);
			}
		}
		for (Map.Entry<String, String> f : eventA.entrySet()) {
			/*if (compare.contains(f.getValue())) {
				statement
				.executeUpdate("UPSERT INTO SAFEEVENTA(SETA,SETB) VALUES ('"
						+ f.getKey() + "','" + f.getValue() + "')");
				connect.commit();
			}*/
			HashSet<String> d=compare(f.getValue());
			if (!(d.isEmpty())) {
				StringBuilder r=new StringBuilder();
				int k=0;
				for(String s:d)
				{   if(k==0)
				   {
					r.append(s);
					k++;
				   }
				else
				{
					r.append(","+s);
				}
				}
			
				statement
				.executeUpdate("INSERT INTO safeEventA(setA,setB) VALUES ('"
						+ f.getKey() + "','" + r.toString() + "')");
				
				
			}
		}
		System.out.println();
		System.out.println("SafeevenntB");
		for (Map.Entry<String, String> f : eventB.entrySet())

		{
			/*if (compare.contains(f.getValue())) {
				statement
				.executeUpdate("UPSERT INTO SAFEEVENTB(SETA,SETB) VALUES ('"
						+ f.getKey() + "','" + f.getValue() + "')");
				connect.commit();
			}*/
			
			HashSet<String> d=compare(f.getValue());
			if (!(d.isEmpty())) {
				StringBuilder r=new StringBuilder();
				int k=0;
				for(String s:d)
				{   if(k==0)
				   {
					r.append(s);
					k++;
				   }
				else
				{
					r.append(","+s);
				}
				}
				
				
				statement
				.executeUpdate("INSERT INTO safeEventB(setA,setB) VALUES ('"
						+ r.toString() + "','" + f.getKey() + "')");
			
				
			}
		}
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
	}
	public static HashSet<String> compare(String f)
	{
		
			String comp[]=f.split(";");
            int flag=0;
			int i=0;
			ArrayList<String> c=new ArrayList<String>();
			HashSet<String> n=new HashSet<String>();
			if(comp.length>1)
			{
			for(String s:comp)
			{
				for(int r=i+1;r<comp.length;r++)
				{
					c.add(s+";"+comp[r]);
					
				}
				i++;
			}
			
		   for(String m:c)
		   {
			   if(compare.contains(m))
			   {
				   String[] g=m.split(";");
				   n.add(g[0].trim());
				   n.add(g[1].trim());
			   }
			  
		  
		  
		
		}
	
	
}
			return n;
	}
}