package garn.hadoop;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

public class CountDetailLogMapper extends Mapper<Object, Text, Text, IntWritable> {

	private static final Logger LOG = Logger.getLogger(CountDetailLogMapper.class);
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
		    String[] rowServiceLog =  null;
			for (int i = 0; i < lcount; i++) {
				if(!START.equals(lineArr[i]) && !END.equals(lineArr[i])){
					try {
						
						String rowServiceLogs = lineArr[i];
						rowServiceLog = rowServiceLogs.split(PIPE);
						
						
						
						String DETAIL_TYPE = ProcessLogService.ModifyInput(rowServiceLog[4] ,"DETAIL_TYPE");
						String STATE = ProcessLogService.ModifyInput(rowServiceLog[5],"STATE");
						String IN_TIME = ProcessLogService.ModifyInput(rowServiceLog[6],"IN_TIME");
						String OUT_TIME = ProcessLogService.ModifyInput(rowServiceLog[7],"OUT_TIME");
						String DIFF_TIME = ProcessLogService.ModifyInput(rowServiceLog[8],"DIFF_TIME");
						String SSID = ProcessLogService.ModifyInput(rowServiceLog[9],"SSID");
						String INVOKE_PARENT = ProcessLogService.ModifyInput(rowServiceLog[10],"INVOKE_PARENT");
						String INVOKE = ProcessLogService.ModifyInput(rowServiceLog[11],"INVOKE");
						String MOBILE_NO = ProcessLogService.ModifyInput(rowServiceLog[12],"MOBILE_NO");
						String RESULTCODE = ProcessLogService.ModifyInput(rowServiceLog[14],"RESULT_CODE");
						String RESULTDESC = ProcessLogService.ModifyInput(rowServiceLog[15],"RESULT_DESC");
						String AUDITLOGID = ProcessLogService.ModifyInput(rowServiceLog[16],"AUDIT_LOG_ID");
						String CHANNEL = ProcessLogService.ModifyInput(rowServiceLog[17],"CHANNEL");
	
						
						String sql = " INSERT INTO GARN_LOG_DETAIL VALUES( GARN_LOG_DETAIL_SEQ.NEXTVAL , ? , ? , TO_TIMESTAMP(?, 'yyyymmdd hh24:mi:ss.FF') , TO_TIMESTAMP(?, 'yyyymmdd hh24:mi:ss.FF') , ? , ? , ? , ? , ? , ? , ? ,? , ? ) ";
						PreparedStatement st = conn.prepareStatement(sql);
						st.setString(1,DETAIL_TYPE);
						st.setString(2,STATE);
						st.setString(3,IN_TIME);
						st.setString(4,OUT_TIME);
						st.setString(5,DIFF_TIME);
						st.setString(6,SSID);
						st.setString(7,INVOKE_PARENT);
						st.setString(8,INVOKE);
						st.setString(9,MOBILE_NO);
						st.setString(10,RESULTCODE);
						st.setString(11,RESULTDESC);
						st.setString(12,AUDITLOGID);
						st.setString(13,CHANNEL);
						

			
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
