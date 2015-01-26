package mysql_phoenix;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class Relation {
	  public Connection connect = null;
	  public Statement statement = null;
	  public ResultSet resultSet = null;
	  public static HashMap<String,HashMap<String,relation1>> relation=new HashMap<String,HashMap<String,relation1>>();
	  public ArrayList<String> arr=new ArrayList<String>();
	  public ArrayList<String> final_value=new ArrayList<String>();	
	  public void read_database(HashMap<String,ArrayList<String>> trace) throws Exception
	{
		try {
		      
		      Class.forName("com.mysql.jdbc.Driver");
		     
		      connect = DriverManager.getConnection("jdbc:mysql://localhost/bpi_2014?"
		              + "user=root&password=root");

		      statement = connect.createStatement();
		      //statement.executeUpdate("CREATE TABLE preprocess(events varchar(50) ,InProgress varchar(50),InCall varchar(50),Resolved varchar(50),Wait varchar(50),WaitImplementation varchar(50),WaitUser varchar(50),WaitVendor varchar(50),WaitCustomer varchar(50),Unmatched varchar(50),Closed varchar(50),Cancelled varchar(50),AwaitingAssignment varchar(50),Assigned varchar(50))");
		      resultSet = statement.executeQuery("select * from bpi_2014.totalevents");
		      System.out.println("Started make realtion");
		      make_relation(resultSet); 
		      System.out.println("Started set relation");
		      setRelation(trace);
		      
		    } catch (Exception e) {
		      
		    	  throw e;
		      
		    } 
		    finally {
		      connect.close();
		      statement.close();
		      resultSet.close();
		    }
	}
	
	
	public void make_relation(ResultSet result) throws Exception
	{   
	    ArrayList<String> s=new ArrayList<String>();
		while(result.next())
		{
			s.add(result.getString(1));
		}
		Iterator<String> outer=s.iterator();
		while(outer.hasNext())
		{
			HashMap<String,relation1> inner=new HashMap<String,relation1>();
			Iterator<String> in=s.iterator();
			while(in.hasNext())
			{
				inner.put(in.next(), relation1.NOT_CONNECTED);
			}
			relation.put(outer.next(),inner);
		}
		/*for(Map.Entry<String, HashMap<String,String>> outermap:relation.entrySet())
		{
			System.out.println(outermap.getKey());
			HashMap<String,String> in=outermap.getValue();
			for(Map.Entry<String, String> inner:in.entrySet())
			{
				System.out.println(inner.getKey()+" "+inner.getValue());
			
			}
			System.out.println();
		}*/
	}
	
	public void setRelation(HashMap<String,ArrayList<String>> trace)
	{   HashMap<String,relation1> innerMap=new HashMap<String,relation1>();
	    HashMap<String,relation1> innerMap1=new HashMap<String,relation1>();
	    
		for(Map.Entry<String, ArrayList<String>> f:trace.entrySet())
		{
			for(int i=0,j=1;j<f.getValue().size();j++,i++)
			{
				String value=f.getValue().get(i);
				String value1=f.getValue().get(j);
				if(!(value.equals(value1)))
				{
				if(relation.containsKey(value))
				{
					HashMap<String,relation1> inner=relation.get(value);
					if(inner.get(value1)==relation1.NOT_CONNECTED)
					{
						
						 
						inner.put(value1, relation1.PRECEDES);
						relation.put(value, inner);
						arr.add(value.toString()+">"+value1.toString());
					}
				}
			
				}
			}
		}


				/*String value=f.getValue().get(i);
				//if(i==(traceList.size()-1))
				//	break;
				//int j=i+1 ;

				if(relation.containsKey(value))
				{
					innerMap=relation.get(value);
					String nextEvent=f.getValue().get(i+1);
					//System.out.println("Yes");
					//System.out.println(innerMap.get(nextEvent));
					if(innerMap.get(nextEvent)==relation1.NOT_CONNECTED && !(nextEvent.equals(value)))
					{

						innerMap.put(nextEvent,relation1.PRECEDES);
						relation.put(value, innerMap);
						innerMap=relation.get(nextEvent);
						
								
								innerMap.put(value, relation1.FOLLOWS);
						        relation.put(nextEvent,innerMap);
						
					}
					else if(innerMap.get(nextEvent)==relation1.FOLLOWS&& !(nextEvent.equals(value)))
					{
						
					    innerMap.put(nextEvent, relation1.PARALLEL);
					    relation.put(value, innerMap);
						innerMap=relation.get(nextEvent);
						innerMap.put(value, relation1.PARALLEL);
						relation.put(nextEvent, innerMap);


					}

				}}}*/

		/*for(Map.Entry<String, HashMap<String,relation1>> outermap:relation.entrySet())
		{
			System.out.println(outermap.getKey());
			HashMap<String,relation1> in=outermap.getValue();
			for(Map.Entry<String, relation1> inner:in.entrySet())
			{
				System.out.println(inner.getKey()+" "+inner.getValue());
			
			}
			System.out.println();
			
		}
		
		
		System.out.println();
		for(String s:arr)
		{
			System.out.println(s);
			System.out.println();
		}*/
		
	    casuality();
	}
	

	public void casuality()
	{
        
        for(String s:arr)
        {    String g[]=s.split(">");
             String m=g[1].toString()+">"+g[0].toString();
        	if(arr.contains(m))
        	{
        		
        		final_value.add(g[0]+"<-"+g[1]);
        		final_value.add(g[1]+"<-"+g[0]);
        	}
        	else
        	{
        		
        		final_value.add(g[0]+"->"+g[1]);
        	}
        }
        System.out.println("Started updation");
        updation();
      
	}
		
	public void updation()
	{   HashMap<String,relation1> innerMap=new HashMap<String,relation1>();
        HashMap<String,relation1> innerMap1=new HashMap<String,relation1>();
		for(String s:final_value)
		{
			if(s.contains("->"))
			{
				String g[]=s.split("->");
				if(relation.containsKey(g[0]))
				{
					innerMap=relation.get(g[0]);
					innerMap.put(g[1],relation1.CASUALITY);
					relation.put(g[0],innerMap);
				}
			}
			else if(s.contains("<-"))
			{
				String g[]=s.split("<-");
				if(relation.containsKey(g[0]))
				{
					innerMap=relation.get(g[0]);
					innerMap.put(g[1],relation1.PARALLEL);
					relation.put(g[0],innerMap);
					innerMap1=relation.get(g[1]);
					innerMap1.put(g[0],relation1.PARALLEL);
					relation.put(g[1],innerMap1);
				}
			}
		}
		for(Map.Entry<String, HashMap<String,relation1>> outermap:relation.entrySet())
		{
			
			HashMap<String,relation1> in=outermap.getValue();
			for(Map.Entry<String, relation1> inner:in.entrySet())
			{
				if(inner.getValue()==relation1.NOT_CONNECTED)
				{
			      final_value.add(outermap.getKey().toString()+"#"+inner.getKey().toString());		
				}
			
			}
			//System.out.println();
		
		}
		
		try {
			
	        Casuality c=new Casuality();
	        System.out.println("Started casuality");
	        c.readDataBase(final_value);
	        Not_connected d=new Not_connected();
	        System.out.println("Started not connected");
	        d.readDataBase(final_value);
	        
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
	
