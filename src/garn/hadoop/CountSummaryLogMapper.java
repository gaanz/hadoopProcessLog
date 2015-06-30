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


public class CountSummaryLogMapper extends Mapper<Object, Text, Text, IntWritable> {
	private static final Logger LOG = Logger.getLogger(CountSummaryLogMapper.class);
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
						//String rowServiceLogs = "2015011416:44:30|room-nameappxx|ESERV3G|0|SERVICELOG|SSID=lx7i3pd5k04etj03|AUDIT_LOG_ID=|INVOKE=501_04cc317a-dad6-45e5-914a-751e4f638d7f|INVOKE_PARENT=501|EVENT_LOG_ID=null|EVENT_LOG_NAME=null|SERVICE_ID=5|SERVICE_SYSTEM=SSBAPI|SERVICE_NAME=getNType|REQUEST_MSG=66923350140|REQUEST_DATE=14012015 16:44:30 PM|RESPONSE_CODE=Y|RESPONSE_MSG={\"detail\":{\"networkType\":\"3PE\",\"mobileLocation\":\"Non BOS\",\"spName\":\"awn\",\"chargeMode\":\"1\"},\"resultCode\":\"20000\",\"developerMessage\":\"Ntype Match Success\"}|RESPONSE_DATE=14012015 16:44:30 PM|RESPONSE_FLAG=Y|MOBILE_NO=0923350140|NTYPE=3PE|ADMIN_FLAG=|CHANNEL=eService";
						rowServiceLog = rowServiceLogs.split(PIPE);
						
						
						
						String IN_TIME = ProcessLogService.ModifyInput(rowServiceLog[5],"IN_TIME");
						String OUT_TIME = ProcessLogService.ModifyInput(rowServiceLog[6],"OUT_TIME");
						String DIFF_TIME = ProcessLogService.ModifyInput(rowServiceLog[7],"DIFF_TIME");
						String SSID = ProcessLogService.ModifyInput(rowServiceLog[8],"SSID");
						String INVOKE = ProcessLogService.ModifyInput(rowServiceLog[9],"INVOKE");
						String MOBILE_NO = ProcessLogService.ModifyInput(rowServiceLog[10],"MOBILE_NO");
						String INPUT = ProcessLogService.ModifyInput(rowServiceLog[11],"INPUT");
						String OUTPUT = ProcessLogService.ModifyInput(rowServiceLog[12],"OUTPUT");
						String STATUS = ProcessLogService.ModifyInput(rowServiceLog[13],"STATUS");
						String RESULTCODE = ProcessLogService.ModifyInput(rowServiceLog[14],"RESULT_CODE");
						String RESULTDESC = ProcessLogService.ModifyInput(rowServiceLog[15],"RESULT_DESC");
						String AUDITLOGID = ProcessLogService.ModifyInput(rowServiceLog[16],"AUDIT_LOG_ID");


					

						
						
						String sql = " INSERT INTO GARN_LOG_SUMMARY VALUES( GARN_LOG_SUMMARY_SEQ.NEXTVAL , TO_TIMESTAMP(?, 'yyyymmdd hh24:mi:ss.FF') , TO_TIMESTAMP(?, 'yyyymmdd hh24:mi:ss.FF') , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? ) ";
						PreparedStatement st = conn.prepareStatement(sql);
						st.setString(1,IN_TIME);
						st.setString(2,OUT_TIME);
						st.setString(3,DIFF_TIME);
						st.setString(4,SSID);
						st.setString(5,INVOKE);
						st.setString(6,MOBILE_NO);
						st.setString(7,INPUT);
						st.setString(8,OUTPUT);
						st.setString(9,STATUS);
						st.setString(10,RESULTCODE);
						st.setString(11,RESULTDESC);
						st.setString(12,AUDITLOGID);
						
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