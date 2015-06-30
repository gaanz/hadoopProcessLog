package garn.hadoop;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;


public class CountIncomingLogMapper extends Mapper<Object, Text, Text, IntWritable> {
	private static final Logger LOG = Logger.getLogger(CountIncomingLogMapper.class);
	private final static String START = "Start";
	private final static String END = "End";
	private final static String PIPE = "\\|";
	
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		// JDBC driver name and database URL
		String DB_URL = "jdbc:oracle:thin:@10.252.240.245:1521:campaigndb";

		// Database credentials
		String USER = "neweServ";
		String PASS = "neweServ";

		Connection conn = null;
		Statement stmt = null;
		try {

			Class.forName("oracle.jdbc.OracleDriver");

			java.util.Properties info = new java.util.Properties();
			info.put("user", USER);
			info.put("password", PASS);
			info.put("useUnicode", "true");
			info.put("characterEncoding", "UTF-8");

			conn = DriverManager.getConnection(DB_URL, info);
			conn.setAutoCommit(false);

			String lines = value.toString();
		    String []lineArr = lines.split("\n");
		    int lcount = lineArr.length;
		    String[] rowServiceLog = null;
			for (int i = 0; i < lcount; i++) {
				if(!START.equals(lineArr[i]) && !END.equals(lineArr[i])){
					try {
						
						String rowServiceLogs = lineArr[i];
						
						rowServiceLog = rowServiceLogs.split(PIPE);
										
						String LOGTIME = ProcessLogService.ModifyInput(rowServiceLog[0],"LOGTIME");						
						String SSID= ProcessLogService.ModifyInput(rowServiceLog[5],"SSID");
						String AUDIT_LOG_ID = ProcessLogService.ModifyInput(rowServiceLog[6],"AUDIT_LOG_ID");
						String EVENT_LOG_ID = ProcessLogService.ModifyInput(rowServiceLog[7],"EVENT_LOG_ID");
						String EVENT_LOG_NAME = ProcessLogService.ModifyInput(rowServiceLog[8],"EVENT_LOG_NAME");
						String INVOKE=ProcessLogService.ModifyInput(rowServiceLog[9],"INVOKE");
						String COMMAND=ProcessLogService.ModifyInput(rowServiceLog[10],"COMMAND");
						String DATA=ProcessLogService.ModifyInput(rowServiceLog[11],"DATA");
	
				
						String sql = " INSERT INTO GARN_LOG_INCOMING VALUES( GARN_LOG_INCOMING_SEQ.NEXTVAL , TO_TIMESTAMP(?, 'yyyymmddhh24:mi:ss') , ? , ? , ? , ? ,? , ? , ? )";
						PreparedStatement st = conn.prepareStatement(sql);
						st.setString(1,LOGTIME);
						st.setString(2,SSID);
						st.setString(3,AUDIT_LOG_ID);
						st.setString(4,EVENT_LOG_ID);
						st.setString(5,EVENT_LOG_NAME);						
						st.setString(6,INVOKE);
						st.setString(7,COMMAND);
						st.setString(8,DATA);
					
						st.execute();
						conn.commit();
						word.set("Success");
						context.write(word, one);
					} catch (SQLException se) {
						// Handle errors for JDBC
						se.printStackTrace();
						LOG.info(se);
						LOG.info("DATA : "+rowServiceLog);
						if (conn != null) {
							try {
								conn.rollback();
								word.set("Fail");
								context.write(word, one);
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}

					} catch (Exception e) {
						// Handle errors for Class.forName
						e.printStackTrace();
						LOG.info(e);
						LOG.info("DATA : "+rowServiceLog);
						if (conn != null) {
							try {
								word.set("Fail");
								context.write(word, one);
								conn.rollback();

							} catch (SQLException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
						}

					}
				}
				
			}

		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
			if (conn != null) {
				try {
					conn.rollback();
					word.set("Fail");
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
			if (conn != null) {
				try {
					conn.rollback();
					word.set("Fail");
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}

		} finally {
			// finally block used to close resources
			try {
				if (stmt != null)
					conn.close();
			} catch (SQLException se) {
			}// do nothing
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
	}
}

