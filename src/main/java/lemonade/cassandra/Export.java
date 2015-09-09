package lemonade.cassandra;

import java.text.SimpleDateFormat;
import java.util.Iterator;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;


/**
 * Hello world!
 *
 */
public class Export 
{	
    public static void main( String[] args )
    {
    	String keyspace = "ssp";
    	String table = "table_name";
    	
    	String cassandra_host = "my.cassandra.host.com";
    	String cassandra_username = "user";
    	String cassandra_password = "password";
    	
    	//////
    	
    	if (args.length <= 0)
    		return;
    	
    	else if (args[0] != null)
    	{
    		table = args[0];    		
    	}
    	
		Cluster.Builder clusterBuilder = Cluster.builder()
			                .addContactPoints(cassandra_host)
			                .withCredentials(cassandra_username, cassandra_password);
    	Cluster cluster = clusterBuilder.build();
    	Session session	= cluster.connect(keyspace);
    	
    	Statement stmt = new SimpleStatement("SELECT * FROM " + table);
    	stmt.setFetchSize(1000);
    	ResultSet rs = session.execute(stmt);
    	Iterator<Row> iter = rs.iterator();
    	
    	while (!rs.isFullyFetched()) {
    	   rs.fetchMoreResults();
    	   Row row = iter.next();
    	   if (row != null)
    	   {
    		   
    		   // csv hacks: this is just for a poc purpose.
    		   
    		   StringBuilder line = new StringBuilder();
    		   for (Definition key : row.getColumnDefinitions().asList())
    		   {
    			   String val = myGetValue(key, row);
    			   line.append("\"");
    			   line.append(val);
    			   line.append("\"");
    			   line.append(",");    			   
    		   }
    		   line.deleteCharAt(line.length()-1);
    		   System.out.println(line.toString());   
    	   }
    	}
    	
    	session.close();
    	cluster.close();
    	
    }
    
    public static String myGetValue(Definition key, Row row)
    {
    	String str = "";
    	
    	if (key != null)
    	{
    		String col = key.getName();
    		
    		try
    		{
    		if (key.getType() == DataType.cdouble())
    		{
    			str = new Double(row.getDouble(col)).toString();
    		}
    		else if (key.getType() == DataType.cint())
    		{
    			str = new Integer(row.getInt(col)).toString();
    		}
    		else if (key.getType() == DataType.uuid())
    		{
    			str = row.getUUID(col).toString();
    		}
    		else if (key.getType() == DataType.cfloat())
    		{
    			str = new Float(row.getFloat(col)).toString();
    		}
    		else if (key.getType() == DataType.timestamp())
    		{
    			str = row.getDate(col).toString();
    			    			
    			SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
    			str = fmt.format(row.getDate(col));
    			            

    		}
    		else
    		{
    			str = row.getString(col);
    		}
    		} catch (Exception e)
    		{
    			str = "";
    		}
    	}
    	
    	return str;
    }
    
}