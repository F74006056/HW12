import java.io.File;
import java.io.IOException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.util.ArrayList;
 
public class IKDDhw12
{
	public static void main(String[] argv)
	{
		System.out.println("-------- PostgreSQL " + "JDBC Connection Testing ------------"); 
		try
		{
			Class.forName("org.postgresql.Driver");
		}
		catch (ClassNotFoundException e)
		{
			System.out.println("Where is your PostgreSQL JDBC Driver? "	+ "Include in your library path!");
			e.printStackTrace();
			return;
		}
 
		System.out.println("PostgreSQL JDBC Driver Registered!");
		Connection connection = null; 
		try
		{
			connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/postgres", "postgres", "123456");
			Statement st = connection.createStatement();
			//Drop table raindata
			String dropraindataSQL = "DROP TABLE IF EXISTS raindata";
			st.executeUpdate(dropraindataSQL);
			
			String droptempSQL = "DROP TABLE IF EXISTS temp";
			st.executeUpdate(droptempSQL);
			
			//Create table raindata
			String creatertableSQL = "CREATE TABLE rainData (SiteId varchar(20), SiteName varchar(20), County varchar(20), Township varchar(20), TWD67Lon decimal(8, 4), TWD67Lat decimal(7, 4), Rainfall10min decimal(8, 2), Rainfall1hr decimal(8, 2), Rainfall3hr decimal(8, 2), Rainfall6hr decimal(8, 2), Rainfall12hr decimal(8, 2), Rainfall24hr decimal(8, 2), Now varchar(20), Unit varchar(20), PublishTime varchar(20))";
			st.executeUpdate(creatertableSQL);
			
			//Get CSV filenames
			File path = new File("data");
        	ArrayList<String> fileList = new ArrayList<String>();
        	if(path.isDirectory())
        	{
		        String []s=path.list();
		        for(int i=0;i<s.length;i++)
		        {
		        	if(s[i].substring(0, 4).equals("Rain"))
		        		fileList.add(s[i]); 
		        }
		    }
        	//Read CSV files        	
		    for(int i=0;i<fileList.size();i++)
		    {
		        String copydbSQL = "COPY raindata FROM '/home/maikaze/data/"+ fileList.get(i) + "' DELIMITERS ',' CSV HEADER";
		        st.executeUpdate(copydbSQL);
		    }		    
		    
		    String selectdistictrSQL = "select distinct * into temp from raindata";
			st.executeUpdate(selectdistictrSQL);
			String dropraindata = "drop table raindata";
			st.executeUpdate(dropraindata);
			String selectdisticttSQL = "select * into raindata from temp";
			st.executeUpdate(selectdisticttSQL);
			String droptemp = "drop table temp";
			st.executeUpdate(droptemp);
		    
			int counter = 0;
			String selectdbSQL = "SELECT siteId, SUM(rainfall10min) FROM raindata GROUP BY siteId HAVING SUM(rainfall10min)>0";
			ResultSet selectdbResult = st.executeQuery(selectdbSQL);
			System.out.println("SiteID\t" + "SUM");
			while(selectdbResult.next())
			{
				System.out.println(selectdbResult.getString("siteId") + "\t" + selectdbResult.getString("sum"));
				counter++;
			}
			System.out.println("Total number:" + counter);
			//Select the MAX rainfall10min
			String maxdbSQL = "SELECT siteId, SUM(rainfall10min) maxrain FROM raindata GROUP BY siteId ORDER BY SUM(rainfall10min) DESC LIMIT 1";
			ResultSet maxdbResult = st.executeQuery(maxdbSQL);
			while(maxdbResult.next())
				System.out.println("Max:" + maxdbResult.getString("siteId"));
			//Add column geom
			String addcolumnSQL = "ALTER TABLE raindata ADD COLUMN geom geometry(point, 4326)";
			st.executeUpdate(addcolumnSQL);
			//Set value for geom
			String setGeomSQL = "UPDATE raindata SET geom = ST_SetSRID(ST_MakePoint(TWD67Lon, TWD67Lat), 4326)";
			st.executeUpdate(setGeomSQL);
			String geoDisSQL = "SELECT siteId FROM raindata WHERE ST_DWithin(geom, ST_GeomFromText('POINT(120.221341 22.997255)', 4326), 0.1 ) GROUP BY siteID";
			ResultSet distanceResult = st.executeQuery(geoDisSQL);
			System.out.println("SiteID");
			while(distanceResult.next())
				System.out.println(distanceResult.getString("siteId"));
			//Close connection
		    connection.close();
		}
		
		catch (SQLException e)
		{
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}
 
		if (connection == null)
			System.out.println("Failed to make connection!");
	}
 
}
