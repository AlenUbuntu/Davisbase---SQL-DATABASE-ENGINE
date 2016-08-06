package davisDB;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import org.omg.CORBA.Any;
import org.omg.CORBA.ORB;

import davisDB.CommandTree.Node;

import java.time.ZonedDateTime;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;

public class Utilities {
	
	public static enum TypeCode{
		/* serial type code occupies 2 byte each*/
		NULL_1(0x00,1),NULL_2(0x01,2),NULL_3(0x02,4),NULL_4(0x03,8),TINYINT(0x04,1),SMALLINT(0x05,2),INT(0x06,4),BIGINT(0x07,8),REAL(0x08,4),DOUBLE(0x09,8),DATETIME(0x0A,8),DATE(0x0B,8),TEXT(0x0C);
		private int typeCode;
		private int size;
		private TypeCode(int typeCode,int size){
			this.typeCode = typeCode;
			this.size = size;
		}
		private TypeCode(int typeCode){
			this.typeCode = typeCode;
		}
		
		public int getTypeCode(){
			return this.typeCode;
		}
		
		public int getSize(){
			return this.size;
		}
	}
	
	public static enum Default{
		NULL(0),TINYINT((byte)0),SMALLINT((short)0),INT(0),BIGINT((long)0),REAL((float)0.0),DOUBLE(0.0),DATETIME(Utilities.toEpochMillSeconds(ZonedDateTime.now(ZoneId.systemDefault()))),DATE(Utilities.toEpochMillSeconds(ZonedDateTime.now(ZoneId.systemDefault()))),TEXT(null);
		private int defaultInt=0;
		private long defaultLong=0;
		private float defaultFloat=(float)0.0;
		private double defaultDouble=0.0;
		private String defaultStr=null;
		private short defaultShort=0;
		private byte defaultByte=0;
		private Default(int defaultInt){
			this.setDefaultInt(defaultInt);
		}
		private Default(byte defaultByte){
			this.setDefaultByte(defaultByte);
		}
		private Default(long defaultLong){
			this.setDefaultLong(defaultLong);
		}
		private Default(float defaultFloat){
			this.setDefaultFloat(defaultFloat);
		}
		private Default(double defaultDouble){
			this.setDefaultDouble(defaultDouble);
		}
		private Default(short defaultShort){
			this.setDefaultShort(defaultShort);
		}
		private Default(String defaultString){
			this.setDefaultStr(defaultString);
		}
		public int getDefaultInt() {
			return defaultInt;
		}
		public void setDefaultInt(int defaultInt) {
			this.defaultInt = defaultInt;
		}
		public long getDefaultLong() {
			return defaultLong;
		}
		public void setDefaultLong(long defaultLong) {
			this.defaultLong = defaultLong;
		}
		public float getDefaultFloat() {
			return defaultFloat;
		}
		public void setDefaultFloat(float defaultFloat) {
			this.defaultFloat = defaultFloat;
		}
		public double getDefaultDouble() {
			return defaultDouble;
		}
		public void setDefaultDouble(double defaultDouble) {
			this.defaultDouble = defaultDouble;
		}
		public String getDefaultStr() {
			return defaultStr;
		}
		public void setDefaultStr(String defaultStr) {
			this.defaultStr = defaultStr;
		}
		public short getDefaultShort() {
			return defaultShort;
		}
		public void setDefaultShort(short defaultShort) {
			this.defaultShort = defaultShort;
		}
		public byte getDefaultByte() {
			return defaultByte;
		}
		public void setDefaultByte(byte defaultByte) {
			this.defaultByte = defaultByte;
		}
	}
	
	// repeat a string for the given times
	public static String repeat(String source,int times){
		int capacity = times*source.length();
		StringBuilder builder = new StringBuilder(capacity);
		for (int i=0;i<times;i++)
			builder.append(source);
		return builder.toString();
	}
	
	// print a table
	public static void printTable(String[] headerList, HashMap<String,String[]> map, int maxLength, int maxRowNum){
		if(maxLength == 0){
			System.out.println("Empty Set");
			return;
		}
		String decoration = Utilities.repeat("-", maxLength);
		System.out.print(" +"+decoration+"+");
		for(int i=0;i<headerList.length-1;i++)
			System.out.print(decoration+"+");
		System.out.println();
		
		for (int i=0;i<headerList.length;i++){
			if(i == 0)
				System.out.print(" |");
			else
				System.out.print("|");
			System.out.print(headerList[i]);

			System.out.print(Utilities.repeat(" ", maxLength-headerList[i].length()));
		}
		System.out.print("|");
		System.out.println();
		
		System.out.print(" +"+decoration+"+");
		for(int i=0;i<headerList.length-1;i++)
			System.out.print(decoration+"+");
		System.out.println();
		
		for (int idx =0;idx<maxRowNum;idx++){
			for (int i=0;i<headerList.length;i++){
				if(i == 0)
					System.out.print(" |");
				else
					System.out.print("|");
				String tmp = map.get(headerList[i])[idx];
				System.out.print(tmp);
				System.out.print(Utilities.repeat(" ", maxLength-tmp.length()));
			}
			System.out.print("|");
			System.out.println();
		}
		
		System.out.print(" +"+decoration+"+");
		for(int i=0;i<headerList.length-1;i++)
			System.out.print(decoration+"+");
		System.out.println();

	}
	
	//get maximum length of an array
	public static int getMaxLength(String[] array){
		int maxLength = 0;
		for (int idx=0;idx<array.length;idx++){
			if (maxLength<array[idx].length())
				maxLength = array[idx].length();
		}
		return maxLength;
	}
	
	// cast integer array to int array
	public static int[] toIntArray(Integer[] array){
		int[] arr = new int[array.length];
		for(int i=0;i<array.length;i++)
			arr[i] = (int) array[i];
		return arr;
	}
	
	public static Integer[] toIntegerArray(int[] array){
		Integer[] arr = new Integer[array.length];
		for (int i=0;i<array.length;i++){
			arr[i] = array[i];
		}
		return arr;
	}
	
	public static ArrayList<Integer> toArrayList(int[] array){
		Integer[] tmp = toIntegerArray(array);
		List<Integer> list = Arrays.asList(tmp);
		return new ArrayList<Integer>(list);
	}
	
	public static int[] removeDupKeys(int[] keys){
		Integer[] tmp = toIntegerArray(keys);

		List<Integer> list = Arrays.asList(tmp);
		HashSet<Integer> set = new HashSet<>(list);
		tmp = set.toArray(new Integer[]{});
		return Utilities.toIntArray(tmp);
	}
	
	// convert a ZonedDateTime to its corresponding epochMillSeconds
	// the epoch is 1970-01-01T00:00:00Z
	
	public static long toEpochMillSeconds(ZonedDateTime time){
		long seconds;
		if(time!=null)
			seconds = time.toEpochSecond();
		else
			seconds = 0;
		return seconds;
	}
	
	// convert a epoch time to its corresponding ZonedDateTime
	public static ZonedDateTime toZonedDateTime(long epochSeconds){
		Instant instant = Instant.ofEpochSecond(epochSeconds);
		ZonedDateTime datetime = ZonedDateTime.ofInstant(instant, ZoneId.systemDefault());
		return datetime;
	}

	public static void parse(Node attrRoot, ArrayList<String> colNames, HashMap<String, String> dataTypes,
			HashMap<String, String[]> constraints) {
		Node p = attrRoot;
		while(p!=null){			
			String name = p.getName();
			String[] tmp = p.getCons();
			String dataType = tmp[0];
			String[] cons = new String[tmp.length-1];
			for (int i=1;i<tmp.length;i++)
				cons[i-1]=tmp[i];
			colNames.add(name);
			dataTypes.put(name, dataType);
			constraints.put(name, cons);
			p = p.getSibl();
		}
		
	}

	public static int getDataTypeLength(String dataType) {
		dataType = dataType.toUpperCase().trim();
		switch(dataType){
		case "TINYINT":
			return Utilities.TypeCode.TINYINT.getSize();
		case "SMALLINT":
			return Utilities.TypeCode.SMALLINT.getSize();
		case "INT":
			return Utilities.TypeCode.INT.getSize();
		case "BIGINT":
			return Utilities.TypeCode.BIGINT.getSize();
		case "REAL":
			return Utilities.TypeCode.REAL.getSize();
		case "DOUBLE":
			return Utilities.TypeCode.DOUBLE.getSize();
		case "DATETIME":
			return Utilities.TypeCode.DATETIME.getSize();
		case "DATE":
			return Utilities.TypeCode.DATE.getSize();
		}
		if (dataType.contains("TEXT"))
			return getCharMaxLength(dataType);
		return 0;
	}


	public static int getCharMaxLength(String dataType) {
		dataType=dataType.toUpperCase().trim();
		if(!dataType.contains("TEXT"))
			return 0;
		int start = dataType.indexOf("(");
		int end = dataType.indexOf(")");
		String type = dataType.substring(start+1, end);
		return Integer.parseInt(type);
	}
	
	public static String getDataType(String dataType){
		dataType = dataType.toUpperCase().trim();
		if(!dataType.contains("TEXT"))
			return dataType;
		else
			return "TEXT";
	}
	
	public static String checkPri(String[] cons) {
		for(int i=0;i<cons.length;i++){
			if(cons[i].toLowerCase().trim().contains("pri"))
				return "PRI";
		}
		return "";
	}

	public static String checkANull(String[] cons){
		for(int i=0;i<cons.length;i++){
			if(cons[i].toLowerCase().trim().contains("not null")||cons[i].toLowerCase().trim().contains("pri"))
				return "NO";
		}
		return "YES";
	}
	
	/*
	 * "" empty string and null is interpreted as 0 in binary file
	 */
	public static String getDefault(String dataType){
		dataType = dataType.toUpperCase().trim();
		if(dataType.contains("TEXT"))
			return Utilities.Default.TEXT.getDefaultStr();
		else{
			switch(dataType){
			case "TINYINT":
				return "NULL";
			case "SMALLINT":
				return "NULL";
			case "INT":
				return "NULL";
			case "BIGINT":
				return "NULL";
			case "REAL":
				return "NULL";
			case "DOUBLE":
				return "NULL";
			case "DATETIME":
				return "NULL";
			case "DATE":
				return "NULL";
			}
		}
		return "";
	}
	
	
	public static int getNumPrec(String dataType) {
		dataType = dataType.toUpperCase().trim();
		switch(dataType){
		case "INT":
			return 10;
		case "TINIYINT":
			return 3;
		case "SMALLINT":
			return 5;
		case "BIGINT":
			return 20;
		case "REAL":
			return 7;
		case "DOUBLE":
			return 15;
		case "DATETIME":
			return 0;
		case "DATE":
			return 0;
		}
		return 0;
	}

	/* key value is the primary key value and should be uique, otherwise an exception is thrown */
	public static HashMap<Integer,Integer> KeyVal2Idx (int[] keyVals){
		HashMap<Integer,Integer> map = new HashMap<>();
		int[] keyValsAft = removeDupKeys(keyVals);
		if (keyValsAft.length != keyVals.length)
			throw new RuntimeException("Error 06: Duplicate key values are found!");
		for(int i=0;i<keyVals.length;i++)
			map.put(keyVals[i], i);
		return map;
	}
	
	
	public static HashMap<Integer,Integer> Key2Pointer(int[] keyVals,int[] pointers){
		HashMap<Integer,Integer> map = new HashMap<>();
		int[] keyValsAft = removeDupKeys(keyVals);
		if (keyValsAft.length != keyVals.length)
			throw new RuntimeException("Error 06: Duplicate key values are found!");
		for(int i=0;i<keyVals.length;i++)
			map.put(keyVals[i], pointers[i]);
		return map;
	}
	
	public static int getColumnSize(int typeCode){
		switch(typeCode){
		case 0x00:
			return 1;
		case 0x01:
			return 2;
		case 0x02:
			return 4;
		case 0x03:
			return 8;
		case 0x04:
			return 1;
		case 0x05:
			return 2;
		case 0x06:
			return 4;
		case 0x07:
			return 8;
		case 0x08:
			return 4;
		case 0x09:
			return 8;
		case 0x0A:
			return 8;
		case 0x0B:
			return 8;
		default:
			if(typeCode>=0x0C){
				return typeCode-0x0C;
			}
		}
		return 0;
	}
	
	public static boolean compare(byte rec_key,byte key,String operator) throws Exception{
		switch(operator){
		case "=":
			return rec_key==key;
		case ">":
			return rec_key>key;
		case "<":
			return rec_key<key;
		case "!=":
			return rec_key!=key;
		case "<>":
			return rec_key!=key;
		case ">=":
			return rec_key>=key;
		case"<=":
			return rec_key<=key;
		default:
			throw new Exception("Error 07: Unsupported comparison operation!");
		}
	}
	
	public static boolean compare(short rec_key,short key,String operator) throws Exception{
		switch(operator){
		case "=":
			return rec_key==key;
		case ">":
			return rec_key>key;
		case "<":
			return rec_key<key;
		case "!=":
			return rec_key!=key;
		case "<>":
			return rec_key!=key;
		case ">=":
			return rec_key>=key;
		case"<=":
			return rec_key<=key;
		default:
			throw new Exception("Error 07: Unsupported comparison operation!");
		}
	}
	
	public static boolean compare(int rec_key,int key,String operator) throws Exception{
		switch(operator){
		case "=":
			return rec_key==key;
		case ">":
			return rec_key>key;
		case "<":
			return rec_key<key;
		case "!=":
			return rec_key!=key;
		case "<>":
			return rec_key!=key;
		case ">=":
			return rec_key>=key;
		case"<=":
			return rec_key<=key;
		default:
			throw new Exception("Error 07: Unsupported comparison operation!");
		}
	}
	
	public static boolean compare(long rec_key,long key,String operator) throws Exception{
		switch(operator){
		case "=":
			return rec_key==key;
		case ">":
			return rec_key>key;
		case "<":
			return rec_key<key;
		case "!=":
			return rec_key!=key;
		case "<>":
			return rec_key!=key;
		case ">=":
			return rec_key>=key;
		case"<=":
			return rec_key<=key;
		default:
			throw new Exception("Error 07: Unsupported comparison operation!");
		}
	}
	
	public static boolean compare(float rec_key,float key,String operator) throws Exception{
		switch(operator){
		case "=":
			return rec_key==key;
		case ">":
			return rec_key>key;
		case "<":
			return rec_key<key;
		case "!=":
			return rec_key!=key;
		case "<>":
			return rec_key!=key;
		case ">=":
			return rec_key>=key;
		case"<=":
			return rec_key<=key;
		default:
			throw new Exception("Error 07: Unsupported comparison operation!");
		}
	}
	
	public static boolean compare(double rec_key,double key,String operator) throws Exception{
		switch(operator){
		case "=":
			return rec_key==key;
		case ">":
			return rec_key>key;
		case "<":
			return rec_key<key;
		case "!=":
			return rec_key!=key;
		case "<>":
			return rec_key!=key;
		case ">=":
			return rec_key>=key;
		case"<=":
			return rec_key<=key;
		default:
			throw new Exception("Error 07: Unsupported comparison operation!");
		}
	}
	
	public static boolean compare(String rec_key,String key,String operator) throws Exception{
		int tmp;
		switch(operator){
		case "=":
			tmp = rec_key.compareTo(key);
			if(tmp == 0) 
				return true;
			else
				return false;
		case ">":
			tmp = rec_key.compareTo(key);
			if(tmp > 0) 
				return true;
			else
				return false;
		case "<":
			tmp = rec_key.compareTo(key);
			if(tmp < 0) 
				return true;
			else
				return false;
		case "!=":
			tmp = rec_key.compareTo(key);
			if(tmp != 0) 
				return true;
			else
				return false;
		case "<>":
			tmp = rec_key.compareTo(key);
			if(tmp != 0) 
				return true;
			else
				return false;
		case ">=":
			tmp = rec_key.compareTo(key);
			if(tmp >= 0) 
				return true;
			else
				return false;
		case"<=":
			tmp = rec_key.compareTo(key);
			if(tmp <= 0) 
				return true;
			else
				return false;
		default:
			throw new Exception("Error 07: Unsupported comparison operation!");
		}
	}
	
	public static boolean In(int a, int[] collection){
		for(int i=0;i<collection.length;i++){
			if(a==collection[i])
				return true;
		}
		return false;
	}
	
	public static int calRecordLength(int[] typeCodes,boolean leaf) {
		int length = 0;
		
		// add deleteMarker
		length += 1;
		
		// add cell header
		if(leaf){
			// add # of bytes of payload
			length+=2;
			
			// add integer key
			length+=4;
		}
		else{
			//add left child pointer
			length+=4;
			
			// add integer key
			length+=4;
			
			return length;
		}
		
		// add payload length
		// add # of bytes in header of payload
		length+=2;
		
		// add length of typeCode series
		length+=typeCodes.length;
		
		// add value 
		for (int i=0;i<typeCodes.length;i++){
			switch(typeCodes[i]){
			
			case 0x00:
				length+=1;
				break;
			case 0x01:
				length+=2;
				break;
			case 0x02:
				length+=4;
				break;
			case 0x03:
				length+=8;
				break;
			case 0x04:
				length+=1;
				break;
			case 0x05:
				length+=2;
				break;
			case 0x06:
				length+=4;
				break;
			case 0x07:
				length+=8;
				break;
			case 0x08:
				length+=4;
				break;
			case 0x09:
				length+=8;
				break;
			case 0x0A:
				length+=8;
				break;
			case 0x0B:
				length+=8;
				break;
			default:
				if(typeCodes[i]>=0x0C)
					length+=(typeCodes[i]-0x0C);
			}
		}
		return length;
	}
	
	public static void printVal(int typeCode,Any any){
		switch(typeCode){
		case 0x00:
			 break;
		case 0x01:
			 break;
		case 0x02:
			 break;
		case 0x03:
			 break;
		case 0x04:
			byte val = any.extract_octet();
			System.out.print(val);
			break;
		case 0x05:
			short val1 = any.extract_short();
			System.out.print(val1);
			break;
		case 0x06:
			int val2 = any.extract_long();
			System.out.print(val2);
			break;
		case 0x07:
			long val3 = any.extract_longlong();
			System.out.print(val3);
			break;
		case 0x08:
			float val4 = any.extract_float();
			System.out.print(val4);
			break;
		case 0x09:
			double val5 = any.extract_double();
			System.out.print(val5);
			break;
		case 0x0A:
			long val6 = any.extract_longlong();
			System.out.print(val6);
			break;
		case 0x0B:
			long val7 = any.extract_longlong();
			System.out.print(val7);
			break;
		default:
			if(typeCode>0x0C){
				String s = any.extract_string();
				System.out.print(s);
				break;
			}
		}
	}

	public static String buildStrWithSize(String s, int remainSize) {
		StringBuilder builder;
		if(s!=null)
			builder = new StringBuilder(s);
		else
			builder = new StringBuilder(remainSize);
		for(int i=0;i<remainSize;i++)
			builder.append(" ");
		return builder.toString();
	}

	public static byte[] convertoBytes(int[] typeCodes, HashMap<Integer, Any> values, boolean leaf, int keyVal) {
		int length = calRecordLength(typeCodes,leaf);

		byte[] result = new byte[length];
		byte[] tmp;
		ByteBuffer buffer;
		int idx = 0;
		if(leaf){
			// write delete marker
			result[idx++]=0;
			//write # of bytes of payload
			buffer = ByteBuffer.allocate(2);
			buffer.putShort((short) (length-1-2-4));
			tmp = buffer.array();
			for(int i=0;i<tmp.length;i++)
				result[idx++] = tmp[i];
			
			//write key value
			buffer = ByteBuffer.allocate(4);
			buffer.putInt(keyVal);
			tmp = buffer.array();
			for(int i=0;i<tmp.length;i++)
				result[idx++]=tmp[i];
			
			//write payload header size
			buffer = ByteBuffer.allocate(2);
			buffer.putShort((short) (typeCodes.length+2));
			tmp = buffer.array();
			for(int i=0;i<tmp.length;i++)
				result[idx++] = tmp[i];
			
			//write payload type code
			for(int i=0;i<typeCodes.length;i++)
				result[idx++]=(byte) typeCodes[i];
			
			//write payload value
			for(int i=0;i<typeCodes.length;i++){
				idx = convertToByteVal(idx,result,typeCodes[i],values.get(i));
			}
		}
		else{
			// for internal node, the pointer is put in the values HashMap, <0,leftPointer>
			// write delete marker
			result[idx++]=0;
			
			// write left pointer
			buffer = ByteBuffer.allocate(4);
			buffer.putInt(values.get(0).extract_long());
			tmp = buffer.array();
			for(int i=0;i<tmp.length;i++)
				result[idx++] =  tmp[i];
			
			// write integer key
			buffer.clear();
			buffer.putInt(keyVal);
			tmp = buffer.array();
			for(int i=0;i<tmp.length;i++)
				result[idx++] = tmp[i];
		}
		return result;
	}

	
	public static int convertToByteVal(int idx, byte[] result, int typeCode, Any any){

		ByteBuffer buffer;
		byte[] tmp;
		switch(typeCode){
		case 0x00:
			 buffer = ByteBuffer.allocate(1);
			 buffer.put((byte) 0);
			 result[idx++] = buffer.array()[0];
			 return idx;
		case 0x01:
			 buffer = ByteBuffer.allocate(2);
			 buffer.putShort((short) 0);
			 tmp = buffer.array();
			 for(int i=0;i<tmp.length;i++)
				 result[idx++] = tmp[i];
			 return idx;
		case 0x02:
			 buffer = ByteBuffer.allocate(4);
			 buffer.putInt(0);
			 tmp = buffer.array();
			 for(int i=0;i<tmp.length;i++)
				 result[idx++] = tmp[i];
			 return idx;
		case 0x03:
			 buffer = ByteBuffer.allocate(8);
			 buffer.putLong(0);
			 tmp = buffer.array();
			 for(int i=0;i<tmp.length;i++)
				 result[idx++] = tmp[i];
			 return idx;
		case 0x04:
			 buffer = ByteBuffer.allocate(1);
			 buffer.put(any.extract_octet());
			 result[idx++] = buffer.array()[0];
			 return idx;
		case 0x05:
			 buffer = ByteBuffer.allocate(2);
			 buffer.putShort(any.extract_short());
			 tmp = buffer.array();
			 for(int i=0;i<tmp.length;i++)
				 result[idx++] = tmp[i];
			 return idx;
		case 0x06:
			 buffer = ByteBuffer.allocate(4);
			 buffer.putInt(any.extract_long());
			 tmp = buffer.array();
			 for(int i=0;i<tmp.length;i++)
				 result[idx++] = tmp[i];
			 return idx;
		case 0x07:
			 buffer = ByteBuffer.allocate(8);
			 buffer.putLong(any.extract_longlong());
			 tmp = buffer.array();
			 for(int i=0;i<tmp.length;i++)
				 result[idx++] = tmp[i];
			 return idx;
		case 0x08:
			 buffer = ByteBuffer.allocate(4);
			 buffer.putFloat(any.extract_float());
			 tmp = buffer.array();
			 for(int i=0;i<tmp.length;i++)
				 result[idx++] = tmp[i];
			 return idx;
		case 0x09:
			 buffer = ByteBuffer.allocate(8);
			 buffer.putDouble(any.extract_double());
			 tmp = buffer.array();
			 for(int i=0;i<tmp.length;i++)
				 result[idx++] = tmp[i];
			 return idx;
		case 0x0A:
			 buffer = ByteBuffer.allocate(8);
			 buffer.putLong(any.extract_longlong());
			 tmp = buffer.array();
			 for(int i=0;i<tmp.length;i++)
				 result[idx++] = tmp[i];
			return idx;
		case 0x0B:
			 buffer = ByteBuffer.allocate(8);
			 buffer.putLong(any.extract_longlong());
			 tmp = buffer.array();
			 for(int i=0;i<tmp.length;i++)
				 result[idx++] = tmp[i];
			 return idx;
		default:
			if(typeCode>0x0C){
				String s = any.extract_string();
				if(s!=null)
					tmp=s.getBytes();
				else
					tmp="null".getBytes();
				for(int i=0;i<tmp.length;i++)
					result[idx++]=tmp[i];		
			}
			return idx;
		}		
	}

	public static void checkConstraints(String[] colNames, HashMap<String, HashMap<String, String>> constraints,
			HashMap<String, byte[]> valueList, ArrayList<Integer> typeCodes, HashMap<Integer,Any> comValList,boolean returnVal) throws Exception {
			HashMap<String,String> cons;
			for(int i=0;i<colNames.length;i++){
				cons = constraints.get(colNames[i]);
				
				// check if missing NOT_NULL value
				if(cons.get("IS_NULLABLE").toLowerCase().trim().equals("no")){

					if(!valueList.containsKey(colNames[i]) || new String(valueList.get(colNames[i])).toLowerCase().trim().equals("null"))
						throw new Exception(String.format("Error 10: Missing required data for %s (NOT NULL)",colNames[i]));
				}
				

				// check if missing Primary key value
				if(cons.get("COLUMN_KEY").toLowerCase().trim().equals("pri")){
					if(!valueList.containsKey(colNames[i]))
						throw new Exception(String.format("Error 11: Missing required data for %s (PRIMARY KEY)",colNames[i]));
				}
				
				// check value type
				
				// get column type
				String column_type = cons.get("DATA_TYPE").toLowerCase().trim();
				// get column default value
				String default_val = cons.get("COLUMN_DEFAULT").toLowerCase().trim();
				// get char max length
				String char_max_length = cons.get("CHAR_MAX_LENGTH").toLowerCase().trim();
				// get value
				
				String value;
				if (valueList.containsKey(colNames[i]))
					value = new String(valueList.get(colNames[i]));
				else
					value = null;
				checkDataType(column_type,char_max_length,value);
			
				if(returnVal)
					setReturnValue(i,column_type,value,default_val,typeCodes,comValList);
			}
	}
	
	private static void setReturnValue(int idx,String column_type, String value, String default_val,
			ArrayList<Integer> typeCodes, HashMap<Integer,Any> comValList) throws Exception {
			org.omg.CORBA.ORB orb = ORB.init();
			Any any;
			switch(column_type){
			case "tinyint":
			    any = orb.create_any();
				if(value!=null){
					if (value.contains("\""))
						value = value.substring(value.indexOf('"')+1,value.lastIndexOf('"'));
					if (value.contains("\'"))
						value = value.substring(value.indexOf('\'')+1,value.lastIndexOf('\''));
					if(value.toLowerCase().trim().equals("null")){
						typeCodes.add(0x00);
						any.insert_octet((byte) 0);
						comValList.put(idx,any);
					}
					else{
						typeCodes.add(0x04);
						any.insert_octet(Byte.parseByte(value));
						comValList.put(idx,any);
					}
				}
				else{
					typeCodes.add(0x00);
					any.insert_octet((byte) 0);
					comValList.put(idx,any);
				}
				break;
			case "smallint":
				any = orb.create_any();
				if(value != null){
					if (value.contains("\""))
						value = value.substring(value.indexOf('"')+1,value.lastIndexOf('"'));
					if (value.contains("\'"))
						value = value.substring(value.indexOf('\'')+1,value.lastIndexOf('\''));
					if(value.toLowerCase().trim().equals("null")){
						typeCodes.add(0x01);
						any.insert_short((short) 0);
						comValList.put(idx,any);
					}
					else{
						typeCodes.add(0x05);
						any.insert_short(Short.parseShort(value));
						comValList.put(idx,any);
					}
				}
				else{
					typeCodes.add(0x01);
					any.insert_short((short) 0);
					comValList.put(idx,any);
				}
			    break;
			case "int":
				any = orb.create_any();
				if(value!=null){
					if (value.contains("\""))
						value = value.substring(value.indexOf('"')+1,value.lastIndexOf('"'));
					if (value.contains("\'"))
						value = value.substring(value.indexOf('\'')+1,value.lastIndexOf('\''));
					if(value.toLowerCase().trim().equals("null")){
						typeCodes.add(0x02);
						any.insert_long(0);
						comValList.put(idx,any);
					}
					else{
						typeCodes.add(0x06);
						any.insert_long(Integer.parseInt(value));
						comValList.put(idx,any);
					}
				}
				else{
					typeCodes.add(0x02);
					any.insert_long(0);
					comValList.put(idx,any);
				}
				break;
			case "bigint":
				any = orb.create_any();
				if(value!=null){
					if (value.contains("\""))
						value = value.substring(value.indexOf('"')+1,value.lastIndexOf('"'));
					if (value.contains("\'"))
						value = value.substring(value.indexOf('\'')+1,value.lastIndexOf('\''));
					if(value.toLowerCase().trim().equals("null")){
						typeCodes.add(0x03);
						any.insert_longlong(0);
						comValList.put(idx,any);
					}
					else{
						typeCodes.add(0x07);
						any.insert_longlong(Long.parseLong(value));
						comValList.put(idx,any);
					}
				}
				else{
					typeCodes.add(0x03);
					any.insert_longlong(0);
					comValList.put(idx,any);
				}
				break;
			case "real":
				
				any = orb.create_any();
				if(value!=null){
					if (value.contains("\""))
						value = value.substring(value.indexOf('"')+1,value.lastIndexOf('"'));
					if (value.contains("\'"))
						value = value.substring(value.indexOf('\'')+1,value.lastIndexOf('\''));
					if(value.toLowerCase().trim().equals("null")){
						typeCodes.add(0x02);
						any.insert_long(0);
						comValList.put(idx,any);
					}
					else{
						typeCodes.add(0x08);
						any.insert_float(Float.parseFloat(value));
						comValList.put(idx,any);
					}
				}
				else{
					typeCodes.add(0x02);
					any.insert_long(0);
					comValList.put(idx,any);
				}
				break;
			case "double":
				any = orb.create_any();
				if(value!=null){
					if (value.contains("\""))
						value = value.substring(value.indexOf('"')+1,value.lastIndexOf('"'));
					if (value.contains("\'"))
						value = value.substring(value.indexOf('\'')+1,value.lastIndexOf('\''));
					if(value.toLowerCase().trim().equals("null")){
						typeCodes.add(0x03);
						any.insert_longlong(0);
						comValList.put(idx,any);
					}
					else{
						typeCodes.add(0x09);
						any.insert_double(Double.parseDouble(value));
						comValList.put(idx,any);
					}
				}
				else{
					typeCodes.add(0x03);
					any.insert_longlong(0);
					comValList.put(idx,any);
				}
				break;
			case "datetime":
				any = orb.create_any();
				if(value!=null){
					if (value.contains("\""))
						value = value.substring(value.indexOf('"')+1,value.lastIndexOf('"'));
					if (value.contains("\'"))
						value = value.substring(value.indexOf('\'')+1,value.lastIndexOf('\''));
					if(value.toLowerCase().trim().equals("null")){
						typeCodes.add(0x03);
						any.insert_longlong(0);
						comValList.put(idx,any);
					}
					else{
						typeCodes.add(0x0A);
						any.insert_longlong(Utilities.parseToZonedDateTime(value, true));
						comValList.put(idx,any);
					}
				}
				else{
					typeCodes.add(0x03);
					any.insert_longlong(0);
					comValList.put(idx,any);
				}
				break;
			case "date":
				any = orb.create_any();
				if(value!=null){
					if (value.contains("\""))
						value = value.substring(value.indexOf('"')+1,value.lastIndexOf('"'));
					if (value.contains("\'"))
						value = value.substring(value.indexOf('\'')+1,value.lastIndexOf('\''));
					if(value.toLowerCase().trim().equals("null")){
						typeCodes.add(0x03);
						any.insert_longlong(0);
						comValList.put(idx,any);
					}
					else{
						typeCodes.add(0x0B);
						any.insert_longlong(Utilities.parseToZonedDateTime(value, false));
						comValList.put(idx,any);
					}
				}
				else{
					typeCodes.add(0x03);
					any.insert_longlong(0);
					comValList.put(idx,any);
				}
				break;
			case "text":
				any = orb.create_any();
				if(value!=null && (!value.contains("\"") && !value.contains("\'"))){
					if(!(value.toLowerCase().trim().equals("null")))
						throw new Exception(String.format("Syntax Error: Missing \" for %s",value));
				}
				if(value!=null){
					if (value.contains("\""))
						value = value.substring(value.indexOf('"')+1,value.lastIndexOf('"'));
					if (value.contains("\'"))
						value = value.substring(value.indexOf('\'')+1,value.lastIndexOf('\''));
					if(value.toLowerCase().trim().equals("null")){
						typeCodes.add(0x0C+"null    ".length());  // reserve 8 bytes for future operation
						any.insert_string("null    ");
						comValList.put(idx,any);
					}
					else{
						typeCodes.add(0x0C+value.length());
						any.insert_string(value);
						comValList.put(idx,any);
					}
				}
				else{
					typeCodes.add(0x0C+default_val.length());
					any.insert_string(default_val);
					comValList.put(idx,any);
				}
				break;
			}
	}
	
	public static long parseToZonedDateTime(String datetime,boolean time) throws Exception{
		if(datetime == null)
			throw new Exception("Error 16: missing datetime value");
		datetime = datetime.trim();
		if(time){
			if(datetime.contains("T")){
				String[] parts  = datetime.split("T");
				datetime = parts[0].trim()+"T"+parts[1].trim();
			}
			else{
				String[] parts  = datetime.split(" ");
				datetime = parts[0].trim()+"T"+parts[1].trim();
			}
			java.time.LocalDateTime tmp = java.time.LocalDateTime.parse(datetime);
			java.time.ZonedDateTime zonedDateTime = tmp.atZone(ZoneId.systemDefault());
			return Utilities.toEpochMillSeconds(zonedDateTime);
		}
		else{
			datetime = datetime+"T"+"00:00:00";
			java.time.LocalDateTime tmp = java.time.LocalDateTime.parse(datetime);
			java.time.ZonedDateTime zonedDateTime = tmp.atZone(ZoneId.systemDefault());
			return Utilities.toEpochMillSeconds(zonedDateTime);
		}
	}

	public static void checkDataType(String column_type,String char_max_length,String value) throws Exception{
		if(value == null)
			return;
		if(value.contains("\""))
			value = value.substring(value.indexOf('"')+1, value.lastIndexOf('"'));
		if (value.contains("\'"))
			value = value.substring(value.indexOf('\'')+1,value.lastIndexOf('\''));
		int charMaxLength = 0;
		if(column_type.contains("text")){
			charMaxLength = Integer.parseInt(char_max_length);
			if(value.length()>charMaxLength)
				throw new Exception("Error 11: text data exceeds specified maximum allowed length");
			return;
		}
		if(column_type.contains("int")){
			if((!value.matches("[0-9]+")) && (!value.toLowerCase().trim().equals("null")))
				throw new Exception("Error 12: Incompatible value provided compared to schema");
			else{
				if(value.toLowerCase().trim().equals("null"))
					return;
				long val = Long.parseLong(value);
				if(column_type.contains("tinyint") && (val>Byte.MAX_VALUE || val <Byte.MIN_VALUE))
					throw new Exception("Error 13: the data provided exceed internal representation range ("+Byte.MIN_VALUE+"~"+Byte.MAX_VALUE+")!");
				if(column_type.contains("smallint") && (val>Short.MAX_VALUE || val<Short.MIN_VALUE))
					throw new Exception("Error 13: the data provided exceed internal representation range ("+Short.MIN_VALUE+"~"+Short.MAX_VALUE+")!");
				if(column_type.contains("int") && (val>Integer.MAX_VALUE || val<Integer.MIN_VALUE))
					throw new Exception("Error 13: the data provided exceed internal representation range ("+Integer.MIN_VALUE+"~"+Integer.MAX_VALUE+")!");
				if(column_type.contains("bigint") && (val>Long.MAX_VALUE || val<Long.MIN_VALUE))
					throw new Exception("Error 13: the data provided exceed internal representation range ("+Long.MIN_VALUE+"~"+Long.MAX_VALUE+")!");
			}	
			return;
		}
		if(column_type.contains("real")){
			if(!value.matches("[0-9.]+") && (!value.toLowerCase().trim().equals("null")))
				throw new Exception("Error 12: Incompatible value provided compared to schema");
			if(value.toLowerCase().trim().equals("null"))
				return;
			float val = Float.parseFloat(value);
			if((val>Float.MAX_VALUE || val<Float.MIN_VALUE))
				throw new Exception("Error 13: the data provided exceed internal representation range ("+Float.MIN_VALUE+"~"+Float.MAX_VALUE+")!");
			return;
		}
		
		if(column_type.contains("double")){
			if(!value.matches("[0-9.]+") && (!value.toLowerCase().trim().equals("null")))
				throw new Exception("Error 12: Incompatible value provided compared to schema");
			if(value.toLowerCase().trim().equals("null"))
				return;
			double val = Double.parseDouble(value);
			if((val>Double.MAX_VALUE || val<Double.MIN_VALUE))
				throw new Exception("Error 13: the data provided exceed internal representation range ("+Double.MIN_VALUE+"~"+Double.MAX_VALUE+")!");
			return;
		}
	}

	public static Any getData(String dataType, String whereValue) throws Exception {
		org.omg.CORBA.ORB orb = ORB.init();
		Any any = orb.create_any();
		dataType = dataType.toLowerCase().trim();
		switch(dataType){
		case "tinyint":
			byte tmp = new Byte(whereValue).byteValue();
			any.insert_octet(tmp);
			return any;
		case "smallint":
			short tmp2 = new Short(whereValue).shortValue();
			any.insert_short(tmp2);
			return any;
		case "int":
			int tmp3 = new Integer(whereValue).intValue();
			any.insert_long(tmp3);
			return any;
		case "bigint":
			long tmp4 = new Long(whereValue).longValue();
			any.insert_longlong(tmp4);
			return any;
		case "real":
			float tmp5 = new Float(whereValue).floatValue();
			any.insert_float(tmp5);
			return any;
		case "double":
			double tmp6 = new Double(whereValue).doubleValue();
			any.insert_double(tmp6);
			return any;
		case "datetime":
			long tmp7 = new Long(Utilities.parseToZonedDateTime(whereValue, true)).longValue();
			any.insert_longlong(tmp7);
			return any;
		case "date":
			long tmp8 = new Long(Utilities.parseToZonedDateTime(whereValue, false)).longValue();
			any.insert_longlong(tmp8);
			return any;
		default:
			any.insert_string(whereValue);
			return any;
		}
	}
	
	public static Any getData(String dataType, String whereValue, int colIndex,HashMap<Integer,Integer> typeCodeList) throws Exception {
		org.omg.CORBA.ORB orb = ORB.init();
		Any any = orb.create_any();
		dataType = dataType.toLowerCase().trim();
		switch(dataType){
		case "tinyint":
			if(whereValue != null && !(whereValue.toLowerCase().trim().equals("null"))){
				typeCodeList.put(colIndex, 0x04);
				byte tmp = new Byte(whereValue).byteValue();
				any.insert_octet(tmp);
			}
			else{
				typeCodeList.put(colIndex,0x00);
				byte tmp = new Byte((byte) 0).byteValue();
				any.insert_octet(tmp);
			}
			return any;
		case "smallint":
			if(whereValue != null && !(whereValue.toLowerCase().trim().equals("null"))){
				typeCodeList.put(colIndex, 0x05);
				short tmp2 = new Short(whereValue).shortValue();
				any.insert_short(tmp2);
			}
			else{
				typeCodeList.put(colIndex, 0x01);
				short tmp2 = new Short((short) 0).shortValue();
				any.insert_short(tmp2);
			}
			return any;
		case "int":
			if(whereValue != null && !(whereValue.toLowerCase().trim().equals("null"))){
				typeCodeList.put(colIndex, 0x06);
				int tmp3 = new Integer(whereValue).intValue();
				any.insert_long(tmp3);
			}
			else{
				typeCodeList.put(colIndex, 0x02);
				int tmp3 = new Integer(0).intValue();
				any.insert_long(tmp3);
			}
			return any;
		case "bigint":
			if(whereValue != null && !(whereValue.toLowerCase().trim().equals("null"))){
				typeCodeList.put(colIndex, 0x07);
				long tmp4 = new Long(whereValue).longValue();
				any.insert_longlong(tmp4);
			}
			else{
				typeCodeList.put(colIndex, 0x03);
				long tmp4 = new Long(0).longValue();
				any.insert_longlong(tmp4);
			}
			return any;
		case "real":
			if(whereValue != null && !(whereValue.toLowerCase().trim().equals("null"))){
				typeCodeList.put(colIndex, 0x08);
				float tmp5 = new Float(whereValue).floatValue();
				any.insert_float(tmp5);
			}
			else{
				typeCodeList.put(colIndex, 0x02);
				float tmp5 = new Float(0).floatValue();
				any.insert_float(tmp5);
			}
			return any;
		case "double":
			if(whereValue != null && !(whereValue.toLowerCase().trim().equals("null"))){
				typeCodeList.put(colIndex, 0x09);
				double tmp6 = new Double(whereValue).doubleValue();
				any.insert_double(tmp6);
			}
			else{
				typeCodeList.put(colIndex, 0x03);
				double tmp6 = new Double(0).doubleValue();
				any.insert_double(tmp6);
			}
			return any;
		case "datetime":
			if(whereValue != null && !(whereValue.toLowerCase().trim().equals("null"))){
				typeCodeList.put(colIndex, 0x0A);
				long tmp7 = new Long(Utilities.parseToZonedDateTime(whereValue,true)).longValue();
				any.insert_longlong(tmp7);
			}
			else{
				typeCodeList.put(colIndex, 0x03);
				long tmp7 = new Long(0).longValue();
				any.insert_longlong(tmp7);
			}
			return any;
		case "date":
			if(whereValue != null && !(whereValue.toLowerCase().trim().equals("null"))){
				typeCodeList.put(colIndex, 0x0B);
				long tmp8 = new Long(Utilities.parseToZonedDateTime(whereValue,false)).longValue();
				any.insert_longlong(tmp8);
			}
			else{
				typeCodeList.put(colIndex, 0x03);
				long tmp8 = new Long(0).longValue();
				any.insert_longlong(tmp8);
			}
			return any;
		default:
			typeCodeList.put(colIndex, 0x0C+whereValue.length());
			any.insert_string(whereValue);
			return any;
		}
	}
	
	public static String getStrData(Any any, int typeCode){
		switch(typeCode){
		case 0x00:
			 return "null";
		case 0x01:
			 return "null";
		case 0x02:
			 return "null";
		case 0x03:
			 return "null";
		case 0x04:
			byte val = any.extract_octet();
			return Byte.toString(val);
		case 0x05:
			short val1 = any.extract_short();
			return Short.toString(val1);
		case 0x06:
			int val2 = any.extract_long();
			return Integer.toString(val2);
		case 0x07:
			long val3 = any.extract_longlong();
			return Long.toString(val3);
		case 0x08:
			float val4 = any.extract_float();
			return Float.toString(val4);
		case 0x09:
			double val5 = any.extract_double();
			return Double.toString(val5);
		case 0x0A:
			long val6 = any.extract_longlong();
			java.time.ZonedDateTime datetime = toZonedDateTime(val6);
			return datetime.toLocalDateTime().toString();
		case 0x0B:
			long val7 = any.extract_longlong();
			java.time.ZonedDateTime datetime2 = toZonedDateTime(val7);
			return datetime2.toLocalDate().toString();
		default:
			if(typeCode>0x0C){
				String s = any.extract_string();
				return s;
			}
		}
		return null;
	}
}

// self-implemented multi-map allows duplicated key values
class MultiMap<K,V> extends java.util.HashMap{
	private HashMap<K,V[]> map = new HashMap<>();
	
	@SuppressWarnings("unchecked")
	public V[] putM(K key, V val){
		V[] val1 = null;
		if (map.containsKey(key))
			val1 = (V[]) map.get(key);
		else
			val1 = null;
		V[] tmp;
		if(val1!=null){
			tmp = (V[]) new Object[val1.length+1];
			for (int i=0;i<val1.length;i++)
				tmp[i] = val1[i];
			tmp[val1.length] = val;
			map.put(key, tmp);
		}
		else{
			tmp = (V[]) new Object[1];
			tmp[0] = val;
			map.put(key, tmp);
		}	
		return val1;
	}
	
	@SuppressWarnings("unchecked")
	public V[] getM(K key){
		if(map.containsKey(key)){
			Object[] arr = map.get(key);
			return (V[]) Arrays.copyOf(arr, arr.length,Integer[].class);
		}
		else
			return null;
	}
}


