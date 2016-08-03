package davisDB;
import java.io.*;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.Scanner;

import org.omg.CORBA.Any;
import org.omg.CORBA.ORB;

import java.util.HashMap;
import java.util.LinkedList;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.ArrayList;

import davisDB.CommandTree.Node;

public class CommandExecutor {
	private String database = null;
	private String engine = "davisDB";
	private String charSetName = "utf-8";
	
	public void execute(CommandTree tree) throws Exception{
		Node root = tree.getRoot();
		int typeCode = root.getCode();
		switch(typeCode){
		case 0:
			execute_ShowTables(root);
			break;
		case 1:
			execute_CreateTable(root);
			break;
		case 2:
			execute_CreateIndex(root);
			break;
		case 3:
			execute_DropTable(root);
			break;
		case 4:
			execute_DropIndex(root);
			break;
		case 5:
			execute_InsertIntoTable(root);
			break;
		case 6:
			execute_DeleteFrom(root);
			break;
		case 7:
			execute_Update(root);
			break;
		case 8:
			execute_Select(root);
			break;
		case 9:
			execute_CreateDatabase(root);
			break;
		case 10:
			execute_UseDatabase(root);
			break;
		case 11:
			execute_ShowSchemas(root);
			break;
		default:
			throw new Exception("Error 03: Unsupported Command! Please type 'help;' for information.");
		}
	}
	
	private void execute_ShowTables(Node root) throws Exception{
		if(this.database == null){
			System.out.println("No database choosed!\nPlease select a database first!");
			return;
		}
		try{
			if(!(this.database.equals("davisbase_schemas"))){
				String cwd = System.getProperty("user.dir");
				String path = cwd + "/davisbase_schemas";
				FileHandler table_handler = new FileHandler(path+"/davisbase_tables"+"/davisbase_tables.tbl",true);
				String[] tableNames = table_handler.getTableNames(table_handler,this.database);
				table_handler.close();
		
				String[] headerList = new String[]{"Table Name"};
				HashMap<String,String[]> map = new HashMap<>();
				map.put(headerList[0], tableNames);
		
				int maxRowNum  = tableNames.length;
				int maxLength = headerList[0].length();
				maxLength = Math.max(maxLength,Utilities.getMaxLength(tableNames));	
				Utilities.printTable(headerList, map, maxLength, maxRowNum);
			}
			else{
				String[] headerList = new String[]{"Table Name"};
				HashMap<String,String[]> map = new HashMap<>();
				String[] tableNames = new String[]{"davisbase_tables","davisbase_columns"};
				map.put(headerList[0], tableNames);
				int maxLength = Utilities.getMaxLength(tableNames);
				maxLength = Math.max(maxLength, headerList[0].length());
				int maxRowNum = tableNames.length;
				Utilities.printTable(headerList, map, maxLength, maxRowNum);
			}
		}
		catch(java.lang.NullPointerException e){
			System.out.println("Empty Set!");
		}
		
	}
	
	private void execute_CreateTable(Node root) throws Exception{
		if(this.database == null){
			System.out.println("No database choosed!\nPlease select a database first!");
			return;
		}
		String cwd = System.getProperty("user.dir");
		String path = cwd + "/davisbase_schemas";
		String tableName = root.getLeft().getName();
		Node attrRoot = root.getLeft().getLeft();
		FileHandler table_handler = new FileHandler(path+"/davisbase_tables"+"/davisbase_tables.tbl",true);
		
		// define values for davisbase_tables.tbl
		String table_catalog = "def";
		String table_schema = this.database;
		String table_type = "basic";
		String row_format = "fixed";
		int table_rows = 0;
		int max_record_length = 0;
		long create_time = Utilities.toEpochMillSeconds(ZonedDateTime.now(ZoneId.systemDefault()));
		long update_time = Utilities.toEpochMillSeconds(null);
		long check_time = Utilities.toEpochMillSeconds(null);
		
		// initialize TypeCode
		int[] typeCodes = new int[11];
		typeCodes[0] = Utilities.TypeCode.TEXT.getTypeCode()+table_catalog.length();
		typeCodes[1] = Utilities.TypeCode.TEXT.getTypeCode()+table_schema.length();
		typeCodes[2] = Utilities.TypeCode.TEXT.getTypeCode()+tableName.length();
		typeCodes[3] = Utilities.TypeCode.TEXT.getTypeCode()+table_type.length();
		typeCodes[4] = Utilities.TypeCode.TEXT.getTypeCode()+this.engine.length();
		typeCodes[5] = Utilities.TypeCode.TEXT.getTypeCode()+row_format.length();
		typeCodes[6] = Utilities.TypeCode.INT.getTypeCode();
		typeCodes[7] = Utilities.TypeCode.INT.getTypeCode();
		typeCodes[8] = Utilities.TypeCode.DATETIME.getTypeCode();
		typeCodes[9] = Utilities.TypeCode.DATETIME.getTypeCode();
		typeCodes[10] = Utilities.TypeCode.DATETIME.getTypeCode();

		
		
		// collect Contents
		HashMap<Integer,Any> values = new HashMap<>();
		org.omg.CORBA.ORB orb = ORB.init();
		
		int idx = 0;
		Any any;
		
		// insert table catalog
		any = orb.create_any();
		any.insert_string(table_catalog);
		values.put(idx++, any);
		
		// insert table schema
		any = orb.create_any();
		any.insert_string(table_schema);
		values.put(idx++, any);
		
		// insert table name
		any = orb.create_any();
		any.insert_string(tableName);
		values.put(idx++, any);
		
		// insert table type
		any = orb.create_any();
		any.insert_string(table_type);
		values.put(idx++, any);
		
		// insert table engine
		any = orb.create_any();
		any.insert_string(this.engine);
		values.put(idx++, any);
		
		// insert row format
		any = orb.create_any();
		any.insert_string(row_format);
		values.put(idx++, any);
		
		// insert table rows
		any = orb.create_any();
		any.insert_long(table_rows);
		values.put(idx++, any);
		
		// insert max_record_length
		any = orb.create_any();
		any.insert_long(max_record_length);
		values.put(idx++, any);
		
		// insert create_time
		any = orb.create_any();
		any.insert_longlong(create_time);
		values.put(idx++, any);
		
		// insert update_time
		any = orb.create_any();
		any.insert_longlong(update_time);
		values.put(idx++, any);
		
		// insert check_time
		any = orb.create_any();
		any.insert_longlong(check_time);
		values.put(idx++, any);
	
		
		table_handler.writeTableRow(typeCodes,values,true,null);
		table_handler.close();
		
		/* write columns */
		FileHandler column_handler = new FileHandler(path+"/davisbase_columns"+"/davisbase_columns.tbl",true);
		
		// restore index
		typeCodes = new int[13];
		
		ArrayList<String> colNames = new ArrayList<>();
		HashMap<String,String> dataTypes = new HashMap<>();
		HashMap<String,String[]> constraints = new HashMap<>();
		
		// parse constraints
		Utilities.parse(attrRoot,colNames,dataTypes,constraints);
		
		// set content
		for(int i=0;i<colNames.size();i++){
			idx = 0;
			values.clear();
			
			typeCodes[0] = Utilities.TypeCode.TEXT.getTypeCode()+table_catalog.length(); 
			typeCodes[1] = Utilities.TypeCode.TEXT.getTypeCode()+table_schema.length();
			typeCodes[2] = Utilities.TypeCode.TEXT.getTypeCode()+tableName.length();
			typeCodes[3] = Utilities.TypeCode.TEXT.getTypeCode()+colNames.get(i).length();
			typeCodes[5] = Utilities.TypeCode.TEXT.getTypeCode()+"YES".length();
			typeCodes[6] = Utilities.TypeCode.TEXT.getTypeCode()+Utilities.getDataType(dataTypes.get(colNames.get(i))).length();
			typeCodes[7] = Utilities.TypeCode.TEXT.getTypeCode()+Integer.toString(Utilities.getCharMaxLength(dataTypes.get(colNames.get(i)))).length();
			typeCodes[8] = Utilities.TypeCode.TEXT.getTypeCode()+Integer.toString(Utilities.getNumPrec(dataTypes.get(colNames.get(i)))).length();
			typeCodes[9] = Utilities.TypeCode.INT.getTypeCode();
			typeCodes[10] = Utilities.TypeCode.TEXT.getTypeCode()+"UTF-16".length();
			typeCodes[11] = Utilities.TypeCode.TEXT.getTypeCode()+dataTypes.get(colNames.get(i)).length();
			typeCodes[12] = Utilities.TypeCode.TEXT.getTypeCode()+"PRI".length();
			
			// insert table catalog
			any = orb.create_any();
			any.insert_string(table_catalog);
			values.put(idx++, any);
		
			// insert table schema
			any = orb.create_any();
			any.insert_string(table_schema);
			values.put(idx++, any);
		
			// insert table name
			any = orb.create_any();
			any.insert_string(tableName);
			values.put(idx++, any);
		
			// insert column name
			any = orb.create_any();
			any.insert_string(colNames.get(i));
			values.put(idx++, any);
		
			// insert column_default 
			any = orb.create_any();
			String tmp = Utilities.getDefault(dataTypes.get(colNames.get(i)));
			if(tmp == null){
				tmp = "NULL";
				any.insert_string(Utilities.buildStrWithSize(tmp, "NULL".length()-tmp.length()));
				typeCodes[4] = Utilities.TypeCode.TEXT.getTypeCode()+"NULL".length();
			}
			else{
				if(tmp.length()<"NULL".length()){
					any.insert_string(Utilities.buildStrWithSize(tmp, "NULL".length()-tmp.length()));
					typeCodes[4] = Utilities.TypeCode.TEXT.getTypeCode()+"NULL".length();
				}
				else{
					any.insert_string(tmp);
					typeCodes[4] = Utilities.TypeCode.TEXT.getTypeCode()+tmp.length();
				}
			}
			values.put(idx++, any);
			

			// insert is_nullable
			any = orb.create_any();
			any.insert_string(Utilities.buildStrWithSize(Utilities.checkANull(constraints.get(colNames.get(i))),"YES".length()-Utilities.checkANull(constraints.get(colNames.get(i))).length()));
			values.put(idx++, any);
		
			// insert data_type
			any = orb.create_any();
			any.insert_string(Utilities.getDataType(dataTypes.get(colNames.get(i))));
			values.put(idx++, any);
		
			// insert char_max_length
			any = orb.create_any();
			any.insert_string(Integer.toString(Utilities.getCharMaxLength(dataTypes.get(colNames.get(i)))));
			values.put(idx++, any);
		
			// insert numeric_prec
			any = orb.create_any();
			any.insert_string(Integer.toString(Utilities.getNumPrec(dataTypes.get(colNames.get(i)))));
			values.put(idx++, any);
		
			// insert numeric_scale
			any = orb.create_any();
			any.insert_long(0);
			values.put(idx++, any);
		
			// insert char_set_name
			any = orb.create_any();
			any.insert_string(Utilities.buildStrWithSize(this.charSetName,"UTF-16".length()-this.charSetName.length()));
			values.put(idx++, any);

			// insert column_type 
			any = orb.create_any();
			any.insert_string(dataTypes.get(colNames.get(i)));
			values.put(idx++, any);

			// insert column_key 
			any = orb.create_any();
			any.insert_string(Utilities.buildStrWithSize(Utilities.checkPri(constraints.get(colNames.get(i))),"PRI".length()-Utilities.checkPri(constraints.get(colNames.get(i))).length()));
			values.put(idx++, any);
			

			column_handler.writeTableRow(typeCodes,values,true,null);

		}	
			column_handler.close();
			
		/* create and write database header of the table */
		path = cwd + "/davisbase_tables" + "/" + this.database;
		FileHandler handler = new FileHandler(path+"/"+tableName+".tbl",true);
		handler.writeDatabaseHeader(handler.getRFile());
		handler.writePageHeader();
		handler.initializePage(1);
		handler.writePageHeader();
		handler.close();
	}
	
	private void execute_CreateIndex(Node root){
		System.out.println("Warning: command currently not supported. It is in the support plan in future versions.");
	}
	
	private void execute_DropTable(Node root) throws Exception{
		if(this.database == null){
			System.out.println("No database selected!\nPlease select a database first!");
			return;
		}
		String cwd = System.getProperty("user.dir");
		String tableName = root.getLeft().getName();
		String path = cwd + "/davisbase_tables" + "/" + this.database;
		FileHandler davisbasetable_handler = new FileHandler(cwd+"/"+"davisbase_schemas"+"/"+"davisbase_tables"+"/"+"davisbase_tables"+".tbl",true);
		FileHandler davisbasecolumn_handler = new FileHandler(cwd+"/"+"davisbase_schemas"+"/"+"davisbase_columns"+"/"+"davisbase_columns"+".tbl",true);

		// delete table file
		try {
		    Files.delete(Paths.get(path+"/"+tableName+".tbl"));
		} catch (NoSuchFileException x) {
		    System.err.format("%s: no such" + " file or directory%n", path+"/"+tableName+".tbl");
		} catch (DirectoryNotEmptyException x) {
		    System.err.format("%s not empty%n", path+"/"+tableName+".tbl");
		} catch (IOException x) {
		    // File permission problems are caught here.
		    System.err.println(x);
		}
		
		// delete table schema
		davisbasetable_handler.updateDelRecs(null, null, true, tableName, davisbasetable_handler, false, false, true, "=", null, -1, false, false, true,null,this.database);
		davisbasecolumn_handler.updateDelRecs(null, null, true, tableName, davisbasecolumn_handler, false, false, true, null, null, -1, false, false, true,null,this.database);
		
	}
	
	private void execute_DropIndex(Node root){
		System.out.println("Warning: command currently not supported. It is in the support plan in future versions.");
	}
	
	private void execute_InsertIntoTable(Node root) throws Exception{
		if(this.database == null){
			System.out.println("No database selected!\nPlease select a database first!");
			return;
		}
		String cwd = System.getProperty("user.dir");
		String tableName = root.getLeft().getName();
		String path = cwd + "/davisbase_tables" + "/" + this.database;
		FileHandler handler = new FileHandler(path+"/"+tableName+".tbl",true);
		Node valRoot = root.getLeft().getLeft();
		
		HashMap<String,HashMap<String,String>> constraints = handler.getTableConstraints(tableName);
		String[] colNames = handler.getTableColNames();
		
		// construct value list
		HashMap<String,byte[]> valueList = new HashMap<>();
		int idx = 0;
		while(valRoot!=null){
			if(valRoot.getName()!=null)
				valueList.put(valRoot.getName(),valRoot.getVal());
			else
				// if no column is provided, it is assumed values are corresponding to first n columns respectively
				valueList.put(colNames[idx++], valRoot.getVal());
			valRoot = valRoot.getSibl();
		}
		ArrayList<Integer> typeCodes = new ArrayList<>();
		HashMap<Integer,Any> comValList = new HashMap<>();
		Utilities.checkConstraints(colNames,constraints,valueList,typeCodes,comValList,true);
		int[] typeCodes_1 = Utilities.toIntArray(typeCodes.toArray(new Integer[]{}));
		handler.writeTableRow(typeCodes_1,comValList,false,tableName);
		handler.close();
	}
	
	private void execute_DeleteFrom(Node root) throws Exception{
	
		
	}
	
	private void execute_Update(Node root) throws Exception{
		if(this.database == null){
			System.out.println("No database selected!\nPlease select a database first!");
			return;
		}
		String cwd = System.getProperty("user.dir");
		String tableName = root.getLeft().getName();
		String path = cwd + "/davisbase_tables" + "/" + this.database;
		FileHandler handler = new FileHandler(path+"/"+tableName+".tbl",true);
		boolean where = false;
		int operIndex = -1;
		Any value = null;
		boolean primary = false;
		boolean priInt = false;
		
		Node valRoot = root.getLeft().getLeft();    // set value
		Node whereRoot = root.getLeft().getSibl();  // where condition
		if(whereRoot!=null)
			where=true;
		
		
		// get constraints
		HashMap<String,HashMap<String,String>> constraints = handler.getTableConstraints(tableName);
		String[] colNames = handler.getTableColNames();
		
		// construct value list
		HashMap<Integer,Any> valueList = new HashMap<>();
		ArrayList<Integer> indexes_tmp = new ArrayList<>();
		HashMap<Integer,Integer> typeCodeList = new HashMap<>();
		while(valRoot!=null){
			String colName = valRoot.getName();
			int colIndex=-1;		
			for(int i=0;i<colNames.length;i++)
				if(colNames[i].equals(colName)){
					colIndex=i;
					break;
				}
				indexes_tmp.add(colIndex);
				String tmp = new String(valRoot.getVal());
				if(tmp.contains("\""))
					tmp = tmp.substring(tmp.indexOf('"')+1,tmp.lastIndexOf('"'));
				HashMap<String,String> map1 = constraints.get(colName);
				String dataType = map1.get("DATA_TYPE");

				Any val = Utilities.getData(dataType, tmp,colIndex,typeCodeList);
				valueList.put(colIndex, val);	
			valRoot = valRoot.getSibl();
		}
		
		// read where condition
		String whereColName = null;
		String operator = null;
		String whereValue = null;
		HashMap<String, byte[]> whereValList = null;	
		if(where){
			// read where condition
			whereColName = whereRoot.getName();
			operator = whereRoot.getCons()[0];
			whereValue = new String(whereRoot.getVal());
			if(whereValue.contains("\""))
				whereValue = whereValue.substring(whereValue.indexOf('"')+1,whereValue.lastIndexOf('"'));
			whereValList = new HashMap<>();
			whereValList.put(whereColName, whereRoot.getVal());

			// check operIndex
			for(int i=0;i<colNames.length;i++)
				if(whereColName.equals(colNames[i])){
					operIndex = i;
					break;
				}
			// get value
			HashMap<String,String> tmp = constraints.get(whereColName);
			String dataType = tmp.get("DATA_TYPE");

			value = Utilities.getData(dataType,whereValue);
			
			// check if where column is primary or not
			HashMap<String,String> tmp2 = constraints.get(whereColName);
			String column_key = tmp2.get("COLUMN_KEY").toLowerCase().trim();
			if(column_key.equals("pri"))
				primary=true;
			
			// check if primary column is int or not
			for(int idx1=0;idx1<colNames.length;idx1++){
				HashMap<String,String> tmp3 = constraints.get(colNames[idx1]);
				if(tmp3.get("COLUMN_KEY").toLowerCase().trim().equals("pri")){
					String data_type = tmp3.get("DATA_TYPE").toLowerCase().trim();
					if(data_type.contains("int") && !data_type.contains("big"))
						priInt = true;	
				}
			}
		}
		// update value
		handler.updateDelRecs(Utilities.toIntArray(indexes_tmp.toArray(new Integer[]{})),valueList,false,tableName,handler,false,true,false,operator,value,operIndex,priInt,primary,where,typeCodeList,null);
	
	}
	
	private void execute_Select(Node root) throws Exception{
		if(this.database == null){
			System.out.println("No database selected!\nPlease select a database first!");
			return;
		}
		String cwd = System.getProperty("user.dir");
		String tableName = root.getLeft().getName();
		String path;
		FileHandler handler;
		boolean schema;
		boolean where = false;
		ArrayList<HashMap<String,Any>> rows= new ArrayList<>();
		ArrayList<HashMap<String,Integer>> typeCodes = new ArrayList<>();
		
		String whereColName = null;
		String operator = null;
		String whereValue = null;
		HashMap<String, byte[]> whereValList = null;	
		String[] colNames = null;
		String[] selColList = null;
		
		int operIndex = -1;
		Any value = null;
		boolean primary = false;
		boolean priInt = false;
		
		if(!tableName.equals("davisbase_tables") && !tableName.equals("davisbase_columns")){
			path = cwd + "/davisbase_tables" + "/" + this.database;
			handler = new FileHandler(path+"/"+tableName+".tbl",true);
			schema =false;
		}
		else{
			path = cwd + "/davisbase_schemas" + "/"+ tableName;
			handler = new FileHandler(path+"/"+tableName+".tbl",true);
			schema = true;
		}
		
		if(!schema){
			Node selectRoot = root.getLeft().getLeft();    // set select root
			Node whereRoot = root.getLeft().getSibl();  // where condition
			if(whereRoot!=null)
				where=true;
		
			// get constraints
			HashMap<String,HashMap<String,String>> constraints = handler.getTableConstraints(tableName);
			colNames = handler.getTableColNames();
		
			// construct selection list
			ArrayList<String> tmp_colList = new ArrayList<>();
			while(selectRoot!=null){
				String tmp = selectRoot.getName();
				if(tmp.equals("*")){
					selColList = colNames;
					break;
				}
				else{
					tmp_colList.add(tmp);
				}
				selectRoot = selectRoot.getSibl();
			}
			if(!tmp_colList.isEmpty())
				selColList = tmp_colList.toArray(new String[]{});
			
			
			if(where){
			// read where condition
				whereColName = whereRoot.getName();
				operator = whereRoot.getCons()[0];
				whereValue = new String(whereRoot.getVal());
				if(whereValue.contains("\""))
					whereValue = whereValue.substring(whereValue.indexOf('"')+1,whereValue.lastIndexOf('"'));
				whereValList = new HashMap<>();
				whereValList.put(whereColName, whereRoot.getVal());
		
			
				// check operIndex
				for(int i=0;i<colNames.length;i++)
					if(whereColName.equals(colNames[i])){
						operIndex = i;
						break;
					}
				// get value
				HashMap<String,String> tmp = constraints.get(whereColName);
				String dataType = tmp.get("DATA_TYPE");
				value = Utilities.getData(dataType,whereValue);
			
				// check if where column is primary or not
				HashMap<String,String> tmp2 = constraints.get(whereColName);
				String column_key = tmp2.get("COLUMN_KEY").toLowerCase().trim();
				if(column_key.equals("pri"))
					primary=true;
			
				// check if primary column is int or not
				for(int idx=0;idx<colNames.length;idx++){
					HashMap<String,String> tmp3 = constraints.get(colNames[idx]);
					if(tmp3.get("COLUMN_KEY").toLowerCase().trim().equals("pri")){
						String data_type = tmp3.get("DATA_TYPE").toLowerCase().trim();
							if(data_type.contains("int") && !data_type.contains("big"))
								priInt = true;	
					}
				}
			}
		}
		if(!schema)
			handler.selectRecs(schema, tableName, handler, operator, value, operIndex, priInt, primary, colNames, where, rows, typeCodes);
		else{
			ArrayList<Integer> keys = new ArrayList<>();
			String[] colNames1;
			if(tableName.equals("davisbase_columns"))
				colNames1 = FileHandler.davisbase_column_header;
			else
				colNames1 = FileHandler.davisbase_table_header;
			handler.getTableRows(null, null, handler, rows, typeCodes, keys,colNames1, -1, null, true, true, tableName, false);
			selColList=colNames1;
		}
		
		// construct parameters for print table
		// selColList is the headerList
		HashMap<String,String[]> column_map = new HashMap<>();
		int maxLength = 0;
		int maxRows = rows.size();
		for(int i=0;i<selColList.length;i++){
			String[] tmp_columns = new String[rows.size()];
			for(int idx=0;idx<rows.size();idx++){
				HashMap<String,Any> tmp_rows = rows.get(idx);
				HashMap<String,Integer> tmp_typeCode = typeCodes.get(idx);
				tmp_columns[idx] = Utilities.getStrData(tmp_rows.get(selColList[i]), tmp_typeCode.get(selColList[i]));
				if(maxLength<tmp_columns[idx].length())
					maxLength = tmp_columns[idx].length();
				if(maxLength<selColList[i].length())
					maxLength = selColList[i].length();
			}
			column_map.put(selColList[i], tmp_columns);
		}
		Utilities.printTable(selColList, column_map, maxLength, maxRows);
	}
	
	
	@SuppressWarnings("resource")
	private void execute_CreateDatabase(Node root) throws IOException{
		
		String cwd = System.getProperty("user.dir");
		String path = cwd + "/davisbase_schemas";
		String dbName = root.getLeft().getName();
		
		
		/* database schema:
		 * database name	
		 *    name			
		 */
		
		BufferedWriter writer;
		Scanner reader;
		try
		{
			// create the corresponding directory if not exists
			new File(path).mkdirs();
		
			// create or open the file
			File database = new File(path,"schemas");
			
			if (!database.exists()){
				writer = new BufferedWriter(new FileWriter(database));
				writer.append(dbName);
				writer.newLine();
				writer.flush();
				writer.close();
			}
			else{
				reader = new Scanner(database);
				while(reader.hasNextLine()){
					String line = reader.nextLine().toLowerCase().trim();
					if (dbName.equals(line))
						throw new Exception(String.format("Error 05: Can't create database %s: %s already exists",dbName,dbName));
				}
				writer = new BufferedWriter(new FileWriter(database,true));
				writer.append(dbName);
				writer.newLine();
					
				reader.close();
				writer.flush();
				writer.close();
			}	
			// create the corresponding directory for davisbase_table and davisbase_column if not exists
			new File(path+"/davisbase_tables").mkdirs();
			new File(path+"/davisbase_columns").mkdirs();
			
			// create or open the file
			FileHandler handler; 
			// create and write davisbase_tables.tbl
			if (Files.exists(Paths.get(path+"/davisbase_tables"+"/davisbase_tables.tbl")) && Files.exists(Paths.get(path+"/davisbase_columns"+"/davisbase_columns.tbl")))
				return;
			handler = new FileHandler(path+"/davisbase_tables"+"/davisbase_tables.tbl",true);
			handler.writeDatabaseHeader(handler.getRFile());
			handler.writePageHeader();
			handler.writeSchemaCols(FileHandler.davisbase_table_header);
			handler.close();
			
			// create and write davisbase_columns.tbl
			handler = new FileHandler(path+"/davisbase_columns"+"/davisbase_columns.tbl",true);
			handler.writeDatabaseHeader(handler.getRFile());
			handler.writePageHeader();
			handler.writeSchemaCols(FileHandler.davisbase_column_header);
			handler.close();
			
		}
		catch(Exception e){
			System.out.println(e.toString());
			e.printStackTrace();
		}
	}
	
	private void execute_UseDatabase(Node root){
		String dbName = root.getLeft().getName();
		String cwd = System.getProperty("user.dir");
		String path = cwd + "/davisbase_tables";
		this.database = dbName;
		// create the corresponding directory for this database if not exists
		new File(path+"/"+this.database).mkdirs();
		System.out.println("Reading table information for completion of table and column names\n\nDatabase Changed");
	}
	
	private void execute_ShowSchemas(Node root) throws Exception{
		String cwd = System.getProperty("user.dir");
		String path = cwd + "/davisbase_schemas";
		LinkedList<String> rows = new LinkedList<>();
		int maxLength = 0;
		
		Scanner reader;
		try{
			reader = new Scanner(new File(path,"schemas"));
			while(reader.hasNextLine()){
				String line = reader.nextLine();
				rows.add(line);
				if(maxLength<line.length())
					maxLength=line.length();
			}
			reader.close();
		}
		catch(FileNotFoundException e){
			System.out.println("No schemas are found!");
			return;
		}
		rows.add("davisbase_schemas");
		String[] databases = rows.toArray(new String[]{});
		maxLength = Utilities.getMaxLength(databases);
		if (maxLength<"Database".length())
			maxLength = "Database".length();
		HashMap<String,String[]> map = new HashMap<>();
		map.put("Database", databases);
		Utilities.printTable(new String[]{"Database"},map, maxLength+1, databases.length);
	}
	
}
