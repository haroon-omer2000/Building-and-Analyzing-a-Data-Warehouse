package meshjoin;
import java.sql.*;
import java.util.*;
import com.google.common.collect.*;


public class meshjoin {
	
	public static class MasterData {
		String PRODUCT_ID;
		String PRODUCT_NAME;
		String SUPPLIER_ID;
		String SUPPLIER_NAME;
		double PRICE;
		
		MasterData(String PRODUCT_ID,String PRODUCT_NAME,String SUPPLIER_ID,String SUPPLIER_NAME,double PRICE){
			this.PRODUCT_ID = PRODUCT_ID;
			this.PRODUCT_NAME = PRODUCT_NAME;
			this.SUPPLIER_ID = SUPPLIER_ID;
			this.SUPPLIER_NAME = SUPPLIER_NAME;
			this.PRICE = PRICE;
		}
	};
	
	public static class TransactionalData {
		int TRANSACTION_ID;
		String PRODUCT_ID;
		String CUSTOMER_ID;
		String CUSTOMER_NAME;
		String STORE_ID;
		String STORE_NAME;
		String T_DATE;
		int QUANTITY;
		
		TransactionalData(){
			this.TRANSACTION_ID = 0;
			this.PRODUCT_ID = null;
			this.CUSTOMER_ID = null;
			this.CUSTOMER_NAME = null;
			this.STORE_ID = null;
			this.STORE_NAME = null;
			this.T_DATE = null;
			this.QUANTITY = 0;
		}
		
		TransactionalData(int TRANSACTION_ID,String PRODUCT_ID,String CUSTOMER_ID,String CUSTOMER_NAME,String STORE_ID,String STORE_NAME,String T_DATE,int QUANTITY, boolean STATUS){
			this.TRANSACTION_ID = TRANSACTION_ID;
			this.PRODUCT_ID = PRODUCT_ID;
			this.CUSTOMER_ID = CUSTOMER_ID;
			this.CUSTOMER_NAME = CUSTOMER_NAME;
			this.STORE_ID = STORE_ID;
			this.STORE_NAME = STORE_NAME;
			this.T_DATE = T_DATE;
			this.QUANTITY = QUANTITY;
		}
	};
	
	static int nextChunkSize(int currentTransNum,int transChunkSize, ResultSet rs, Statement statement) {
		try {
			String query = String.format("SELECT * FROM TRANSACTIONS WHERE TRANSACTION_ID > %s AND TRANSACTION_ID <= %s",currentTransNum,currentTransNum + transChunkSize);
			rs = statement.executeQuery(query);
			int size = 0;
			while(rs.next())
				size = size + 1;
			return size;			
		}
		catch (Exception exc) {
			exc.printStackTrace();
		}
		return 0;
	}
	
	static int calculateQuarter(int month) {
		if (month >= 1 && month <= 3)
			return 1;
		else if (month >= 4 && month <= 6)
			return 2;
		else if (month >= 7 && month <= 9)
			return 3;
		return 4;
	}
	
	static void insertDimensions(boolean isTime,String id,String name, String tname,String date,int day, int month, int year,Connection myConn) {
		try {
			PreparedStatement query;
			if (isTime) {
				int quarter = calculateQuarter(month);
				String tempQuery = String.format("INSERT IGNORE INTO %s (time_id,day,month,quarter,year) values(?,?,?,?,?)",tname);
				query = myConn.prepareStatement(tempQuery);
				query.setString(1, date);
				query.setInt(2, day);
				query.setInt(3, month);
				query.setInt(4, quarter);
				query.setInt(5, year);
			}
			else {
				String tempQuery = String.format("INSERT IGNORE INTO %s values(?,?)",tname); 
				query = myConn.prepareStatement(tempQuery);
				query.setString(1, id);
				query.setString(2, name);
			}
			query.execute();
		}
		catch (Exception exc) {
			exc.printStackTrace();
		}
	}

	static void insertFactTable(String TABLE_NAME,String CUSTOMER_ID,String SUPPLIER_ID,String PRODUCT_ID,String T_DATE,String STORE_ID,int QUANTITY,double TOTAL_SALE, Connection myConn,ResultSet rs,Statement statement) {
		try {
			
			String query1 = String.format("SELECT COUNT(*) AS nRecords FROM %s WHERE ( CUSTOMER_ID = '%s' AND SUPPLIER_ID = '%s' AND PRODUCT_ID = '%s' AND TIME_ID = '%s' AND STORE_ID = '%s' ) ",TABLE_NAME,CUSTOMER_ID, SUPPLIER_ID, PRODUCT_ID, T_DATE,STORE_ID);
			rs = statement.executeQuery(query1); 	
			rs.next();
			if (rs.getInt("nRecords") > 0) {
				System.out.println("Record Exists \n" + CUSTOMER_ID + " " + SUPPLIER_ID + " " + PRODUCT_ID + " " + T_DATE + " " + STORE_ID + "\n");
				query1 = String.format("UPDATE %s set QUANTITY = QUANTITY + ?,TOTAL_SALE = TOTAL_SALE + ? WHERE ( CUSTOMER_ID = ? AND SUPPLIER_ID = ? AND PRODUCT_ID = ? AND TIME_ID = ? AND STORE_ID = ? )",TABLE_NAME);

				PreparedStatement finalQuery = myConn.prepareStatement(query1);
				finalQuery.setInt(1, QUANTITY);
				finalQuery.setDouble(2, TOTAL_SALE);
				finalQuery.setString(3, CUSTOMER_ID);
				finalQuery.setString(4, SUPPLIER_ID);
				finalQuery.setString(5, PRODUCT_ID);
				finalQuery.setString(6, T_DATE);
				finalQuery.setString(7, STORE_ID);
				finalQuery.execute();
			}
			else {
				query1 = String.format("INSERT INTO %s VALUES(?,?,?,?,?,?,?)",TABLE_NAME);
				PreparedStatement finalQuery = myConn.prepareStatement(query1);
				finalQuery.setString(1, CUSTOMER_ID);
				finalQuery.setString(2, SUPPLIER_ID);
				finalQuery.setString(3, PRODUCT_ID);
				finalQuery.setString(4, T_DATE);
				finalQuery.setString(5, STORE_ID);
				finalQuery.setInt(6, QUANTITY);
				finalQuery.setDouble(7, TOTAL_SALE);
				finalQuery.execute();
			}
			
		}
		catch (Exception exc) {
			exc.printStackTrace();
		}
	}
	
	static String[] getProdSuppNames(String prodId, MasterData [][] MD,int TOTAL_PARTITIONS,int MASTER_CHUNK_SIZE) {
		String [] prodSuppNames = new String[3];
		for ( int i = 0 ; i < TOTAL_PARTITIONS; i++ ) {
			for ( int j = 0 ; j < MASTER_CHUNK_SIZE; j++ ) {
				if (prodId.equals(MD[i][j].PRODUCT_ID)) {
					prodSuppNames[0] = MD[i][j].PRODUCT_NAME;
					prodSuppNames[1] = MD[i][j].SUPPLIER_ID; 
					prodSuppNames[2] = MD[i][j].SUPPLIER_NAME; 
					return prodSuppNames;
				}
			}
		}
		return prodSuppNames;
	}

	public static void performRightShift(TransactionalData [][] TD_QUEUE, int TOTAL_PARTITIONS) {		
		for (int i = TOTAL_PARTITIONS - 1; i >= 1 ; i--) 
				TD_QUEUE[i] = TD_QUEUE[i - 1];
	}
	
	public static void performJoin(Multimap<String,TransactionalData> hashTable,ArrayList <Integer> queueHistory,int currPartition,TransactionalData [][] TD_QUEUE,MasterData [][] MD_QUEUE,int MASTER_CHUNK_SIZE,int transChunkSize,int TOTAL_PARTITIONS, Connection myConn, ResultSet rs, Statement statement) {
		
		for (int j = 0;j < MASTER_CHUNK_SIZE;j++) {	
			// if key exists in hash table
			if (hashTable.get(MD_QUEUE[currPartition][j].PRODUCT_ID).size() != 0) {
				
				Collection<TransactionalData> values = hashTable.get(MD_QUEUE[currPartition][j].PRODUCT_ID);
				Iterator<TransactionalData> iterator = values.iterator();
				
				while(iterator.hasNext()) {	
					TransactionalData Temp = iterator.next();
					String PRODUCT_ID = Temp.PRODUCT_ID;
					String CUSTOMER_ID = Temp.CUSTOMER_ID;
					String STORE_ID = Temp.STORE_ID;	
					String T_DATE = Temp.T_DATE;
					int QUANTITY = Temp.QUANTITY;
					
					String [] prodSuppNames = getProdSuppNames(PRODUCT_ID,MD_QUEUE,TOTAL_PARTITIONS,MASTER_CHUNK_SIZE);
					String SUPPLIER_ID = prodSuppNames[1];
					
					double TOTAL_SALE = QUANTITY * MD_QUEUE[currPartition][j].PRICE;
					
					insertFactTable("SALES",CUSTOMER_ID,SUPPLIER_ID,PRODUCT_ID,T_DATE,STORE_ID,QUANTITY,TOTAL_SALE,myConn,rs,statement);
					
					iterator.remove();										
				}
			}
		}
		
	}
	
	public static void main(String[] args) {
		
		try {
			
			int currPartition = 0;
			int currentTransNum = 0;
			int transChunkNum = 1;
			int transChunkSize = 500;
			int TOTAL_PARTITIONS = 10;
			
		    ArrayList <Integer> queueHistory = new ArrayList<Integer>();
			Multimap<String,TransactionalData> hashTable = ArrayListMultimap.create();

			Connection myConn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/dwhproject","root","hackerman1234");
			Statement statement = myConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			/* LOADING MASTER DATA */
			String query = "SELECT COUNT(*) AS COUNT FROM MASTERDATA";
			ResultSet rs = statement.executeQuery(query); 
			
			rs.next();
			int MASTER_DATA_SIZE = rs.getInt("COUNT"); 
			int MASTER_CHUNK_SIZE = MASTER_DATA_SIZE/TOTAL_PARTITIONS;
			MasterData MD_QUEUE [][] = new MasterData[TOTAL_PARTITIONS][MASTER_CHUNK_SIZE];
			TransactionalData TD_QUEUE [][] = new TransactionalData[TOTAL_PARTITIONS][transChunkSize];
			
			// initializing TD_QUEUE with default values
			for (int k = 0;k < TOTAL_PARTITIONS; k++) {
				for (int l = 0;l < transChunkSize; l++) 
					TD_QUEUE[k][l] = new TransactionalData();
			}
			
			// partition master data
			query = "SELECT * FROM MASTERDATA";
			rs = statement.executeQuery(query);
			int i = 0;
			int j = 0;
			while(rs.next()) {
				MD_QUEUE[i][j++] = new MasterData(rs.getString("PRODUCT_ID"),rs.getString("PRODUCT_NAME"),rs.getString("SUPPLIER_ID"),rs.getString("SUPPLIER_NAME"),rs.getDouble("PRICE"));
				if (j % TOTAL_PARTITIONS == 0) {
					i = i + 1;
					j = 0;
				}
			}
						 
			while((nextChunkSize(currentTransNum,transChunkSize, rs, statement) == transChunkSize) || (queueHistory.size() != 0)) {
				
				if (nextChunkSize(currentTransNum,transChunkSize, rs, statement) == transChunkSize) {
					query = String.format("SELECT * FROM TRANSACTIONS WHERE TRANSACTION_ID > %s AND TRANSACTION_ID <= %s",currentTransNum,currentTransNum + transChunkSize);
					rs = statement.executeQuery(query); 
					i = 0;
					while(rs.next()) {
												
						performRightShift(TD_QUEUE,TOTAL_PARTITIONS);
							
						TD_QUEUE[0][i++] = new TransactionalData(rs.getInt("TRANSACTION_ID"),rs.getString("PRODUCT_ID"),rs.getString("CUSTOMER_ID"),rs.getString("CUSTOMER_NAME"),rs.getString("STORE_ID"),rs.getString("STORE_NAME"),rs.getString("T_DATE"),rs.getInt("QUANTITY"),true);
						
						// check if product id is present in master data
						String prodId =  rs.getString("PRODUCT_ID");
						outerloop:
						for (int i1 = 0; i1 < TOTAL_PARTITIONS; i1++) {
							for (int j1 = 0; j1 < MASTER_CHUNK_SIZE ; j1++ ) {
								
								if (prodId.equals(MD_QUEUE[i1][j1].PRODUCT_ID)) {
									
									TransactionalData TD = new TransactionalData(rs.getInt("TRANSACTION_ID"),prodId,rs.getString("CUSTOMER_ID"),rs.getString("CUSTOMER_NAME"),rs.getString("STORE_ID"),rs.getString("STORE_NAME"),rs.getString("T_DATE"),rs.getInt("QUANTITY"),true);																		

									String [] prodSuppNames = getProdSuppNames(prodId,MD_QUEUE,TOTAL_PARTITIONS,MASTER_CHUNK_SIZE); // get product name, supplier id and supplier name									
									
									int day,month,year;									
									String [] date_split = rs.getString("T_DATE").split("-");
									year =  Integer.parseInt(date_split[0]);
									month = Integer.parseInt(date_split[1]);
									day = Integer.parseInt(date_split[2]); 
									
									insertDimensions(false,rs.getString("CUSTOMER_ID"),rs.getString("CUSTOMER_NAME"),"CUSTOMER",null,0,0,0,myConn);
									insertDimensions(false,rs.getString("STORE_ID"),rs.getString("STORE_NAME"),"STORE",null,0,0,0,myConn);
									insertDimensions(false,prodId,prodSuppNames[0],"PRODUCT",null,0,0,0,myConn);
									insertDimensions(true,null,null,"TIME",rs.getString("T_DATE"),day,month,year,myConn);
									insertDimensions(false,prodSuppNames[1],prodSuppNames[2],"SUPPLIER",null,0,0,0,myConn);
																		
									hashTable.put(prodId,TD);									
									break outerloop;
								}
							}
						}
					}
					
					queueHistory.add(0,transChunkNum);
				}
				
				performJoin(hashTable,queueHistory,currPartition,TD_QUEUE,MD_QUEUE,MASTER_CHUNK_SIZE,transChunkSize,TOTAL_PARTITIONS,myConn,rs,statement);
				
				//for (i = 0;i < queueHistory.size();i++) 
					//System.out.println("TD Partition: " + queueHistory.get(i) + " Joined with MD Partition: " + (currPartition + 1));					
				
				if (!(nextChunkSize(currentTransNum,transChunkSize, rs, statement) == transChunkSize)) 
					queueHistory.remove(queueHistory.size() - 1);
				
				//System.out.println("\n");	
			
				currPartition = currPartition + 1;
				currentTransNum = currentTransNum + transChunkSize;
				transChunkNum = transChunkNum + 1;
				
				if(currPartition == 10)
					currPartition = 0;
				
				if (queueHistory.size() == 10)
					queueHistory.remove(TOTAL_PARTITIONS - 1);
				
			}
		}
		catch (Exception exc) {
			exc.printStackTrace();
		}
	}
}