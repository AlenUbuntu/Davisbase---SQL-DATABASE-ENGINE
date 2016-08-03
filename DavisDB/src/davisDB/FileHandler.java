package davisDB;
import java.io.RandomAccessFile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.omg.CORBA.Any;
import org.omg.CORBA.ORB;

import davisDB.MultiMap;

import java.io.File;
import java.io.IOException;

public class FileHandler {
	
	// magic header
	private static String magicHeader = "DavisBase format 1";
	
	// page size
	private static int pageSize = 512;
	
	// B+-tree degree
	private static int degree = 4; // suppose maximum length of TEXT data is 80 characters.
	
	// if readVersion = 1 but write version > 1, the file is read-only
	// if readVersion > 1 the file cannot be read or written
	private static int writeVersion = 1;
	private static int readVersion = 1;
	
	// file change counter
	private int fileChangeCounter = 0;
	
	// database size in pages
	private int databasePages = 0;
	
	// database version number
	private static int version = 1;
	
	// table or index
	private boolean table = true;
	
	// unused bytes at the end of a page, can be used to store overflow file pointer, by default it is 4 bytes
	private int uspace = 4;
	
	// table row container
	private ArrayList<HashMap<String,Any>> rows = null;
	// typeCodes container
	private ArrayList<HashMap<String,Integer>> typeCodes = null;
	// key value corresponding to each row 
	private ArrayList<Integer> keys = null;

	// File 
	private File f;
	
	// RandomAccessFile
	private RandomAccessFile rfile;
	
	// current page
	private Page p = null;
	
	// root of file B+-TREE, second page of the file
	private Page root = null;
	
	// first page of the file containing the database header information
	private Page schemaRoot = null;

	// constructor
	public FileHandler(String filePath,boolean table) throws IOException{
		this.f = new File(filePath);
		this.table = table;
		if (this.f.exists()){
			this.rfile = new RandomAccessFile(f,"rw");
			prepareDatabaseHeader();
			this.schemaRoot = preparePage(0,this);
			this.schemaRoot.schemaRoot=true;
		}
		else
			this.rfile = new RandomAccessFile(f,"rw");
	}
	
	/*
	 * @return the randome access file of this handler
	 */
	public RandomAccessFile getRFile(){
		return this.rfile;
	}
	
	
	/*
	 *  Page Class
 	 */
	private class Page{
		// start of free block for this page, 0 if no free block
		private int freeBlockStart = 0;
	
		// num of cells on this page
		private int cellNum = 0;
	
		// start of cell content area
		private int cellStart = pageSize-1-uspace;
	
		// num of fragmented free bytes within the cell content area
		private int fragNum = 0;
		
		// rightmost page pointer in the page header, space reserved
		private int rightPointer = 0;
		
		// page number, used as page address, start from 0
		private int pagNumber = 0;
		
		
		// leaf 
		private boolean leaf = true;
		
		// page 0 for database header
		private boolean schemaRoot = false;
		
		// parent page
		private Page parentPage = null;
		
		public Page(int pagNum) throws Exception{
			this.pagNumber = pagNum;
			initPage(this.pagNumber);
			if(this.pagNumber == 0)
				this.schemaRoot = true;	
			else if(pagNum>databasePages-1)
				databasePages++;
		}
		
		// constructor but with no initialization
		public Page(int cellNum,int cellStart,int fragNum,int freeBlockStart,boolean leaf, int pagNumber,Page parentPage,int rightPointer, boolean schemaRoot){
			this.cellNum = cellNum;
			this.cellStart = cellStart;
			this.fragNum = fragNum;
			this.freeBlockStart = freeBlockStart;
			this.leaf = leaf;
			this.pagNumber = pagNumber;
			this.parentPage = parentPage;
			this.rightPointer = rightPointer;
			this.schemaRoot = schemaRoot;
		}
		
		
		public void initPage(int pagNumber) throws Exception{
			rfile.seek(pagNumber*FileHandler.pageSize);
			for (int i=0; i<FileHandler.pageSize;i++)
				rfile.writeByte(0);
			rfile.seek(pagNumber*FileHandler.pageSize);
		}
	}
	
	
	
	/*
	 * initialize a page 
	 * @param page number
	 */
	public void initializePage(int pageNum) throws Exception{
		this.p = new Page(pageNum);
		if (pageNum == 1)
			this.root = this.p;
		if(pageNum == 0)
			this.schemaRoot = this.p;
	}
	
	/*
	 * initialize the schema root page
	 */
	public void initializePage() throws Exception{
		initializePage(0);
	}
	
	/*
	 * close the randomAccessFile
	 */
	public void close() throws IOException{
		this.rfile.close();
	}
	
	/* get number of unused byte at the end of a page
	 * @return reserved space size at the end of each page 
	 */
	public void getReservedSpace() throws IOException{
		this.rfile.seek(24);
		this.uspace = this.rfile.readByte();
		this.rfile.seek(0);
	}
	
	/*
	 * get page size of the file
	 */
	public void getPageSize() throws IOException{
		this.rfile.seek(20);
		FileHandler.pageSize = this.rfile.readShort();
		this.rfile.seek(0);
	}
	
	/* 
	 * get file size in pages
	 */
	public void getFileSize() throws IOException{
		this.rfile.seek(28);
		int fileChangeCounter = this.rfile.readInt();
		this.rfile.seek(64);
		int fileChangeCounterAft = this.rfile.readInt();
		if(fileChangeCounter == fileChangeCounterAft){
			this.rfile.seek(32);
			this.databasePages = this.rfile.readInt();
		}
		else{
			recalFilePages();
		}
	}
	
	
	
	/*
	 * 
	 * 0x0000-0x0001: number of bytes for the magic header
	 * 0x0002-0x0013: DavisBase format 1
	 * 0x0014-0x0015: page size
	 * 0x0016: writeVersion
	 * 0x0017: readVersion
	 * 0x0018: unused reserved space, usually 4 (for rightmost pointer of leaf B+-tree)
	 * 0x0019: maximum embedded payload fraction, 64
	 * 0x001A: minimum embedded payload fraction, 32
	 * 0x001B: leaf payload fraction
	 * 0X001C-0X001F: file change counter
	 * 0x0020-0x0023: file size in pages
	 * 0x0024-0x0027: database text encoding
	 * 0x0028-0x003B: reserved space for expansion, 0
	 * 0x003C-0x003F: database version number
	 * 0x0040-0x0043: file change counter immediately after database version number is stored.
	 * 
	 */
	
	public void writeDatabaseHeader(RandomAccessFile rfile) throws Exception{
		if(this.databasePages == 0)
			initializePage();
		
		rfile.seek(0); // move to the schema root page
		
		// write magic header
		rfile.writeUTF(FileHandler.magicHeader);
		
		// write page size
		rfile.writeShort(FileHandler.pageSize);
		
		// write file format version number
		rfile.writeByte(FileHandler.writeVersion);
		rfile.writeByte(FileHandler.readVersion);
		
		// write unused reserved byte, usually 4 for B+-tree leaf node, only valid for leaf B+-tree pages
		rfile.writeByte(this.uspace);
		
		// write maximum embedded payload fraction, must be 64
		rfile.writeByte(64);
		
		// write minimum embedded payload fraction, must be 32
		rfile.writeByte(32);
		
		// write leaf payload fraction
		rfile.writeByte(32);
		
		// write file change counter
		rfile.writeInt(this.fileChangeCounter);
		
		// write database size in pages
		// valid only if it is non-zero and if the file change counter = version-valid-for number
		rfile.writeInt(this.databasePages);
		
		// write database text encoding
		// 1 - ASCII ; 2 - UTF-8 ; 3 - UTF-16
		// default UTF-8
		rfile.writeInt(2);
		
		// write reserved space for expansion, must be 0
		byte[] tmp = new byte[]{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
		this.rfile.write(tmp);
		
		// write library version number - davisbase version number value for the davisbase library that most recently modified the database file
		// write version-valid-for number - value of change counter when the version number was stored.
		this.rfile.writeInt(FileHandler.version);
		this.rfile.writeInt(this.fileChangeCounter);
	}
	
	
	
	/*
	 * recalculate the database pages according to file length
	 * @return number of file pages
	 */
	public int recalFilePages(){
		long numOfBytes = f.length();
		this.databasePages = (int) Math.ceil(numOfBytes/pageSize);
		return this.databasePages;
	}
	
	public int recalFilePages(long numOfBytes){
		this.databasePages = (int) Math.ceil(numOfBytes/pageSize);
		return this.databasePages;
	}
	
	public static void writeTextEncoding(String filePath,int encode) throws Exception{
		RandomAccessFile rfile = new RandomAccessFile(new File(filePath),"rws");
		rfile.seek(0x24);
		rfile.writeInt(encode);
		rfile.seek(0);
		rfile.close();	
	}
	
	public static void writeTextEncoding(File f,int encode) throws Exception{
		RandomAccessFile rfile = new RandomAccessFile(f,"rws");
		rfile.seek(0x24);
		rfile.writeInt(encode);
		rfile.seek(0);
		rfile.close();	
	}
	
	public static void setPageSize(int pageSize){
		FileHandler.pageSize = pageSize;
	}
	
	/*
	 *  write after all other operation completes.
	 */
	public void onSchemaFileCreated() throws Exception{
		this.fileChangeCounter++;
		long totalSize = f.length();
		if (totalSize == 0){
			throw new IOException("file specified does not exist!");
		}
		else{
			recalFilePages(totalSize);
		}
		this.rfile.seek(0x001C);
		this.rfile.writeInt(this.fileChangeCounter);
		this.rfile.seek(0x0020);
		this.rfile.writeInt(this.databasePages);
		this.rfile.seek(0x003C);
		this.rfile.writeInt(FileHandler.version);
		this.rfile.seek(0x0040);
		this.rfile.writeInt(this.fileChangeCounter);
	}
	
	

	/*
	 * Page Header: immediately follow the database header
	 * 0x00: one-byte flag: 0x02 - page is an interior index b-tree page; 0x05 - page is an interior table b-tree page; 0x0a - page is a leaf index b-tree page; 0x0d - page is a leaf table b-tree page
	 * 0x01-0x02: start of the first free block on the page or 0 if there is no free blocks
	 * 0x03-0x04: number of cells on the page
	 * 0x05-0x06: start of the cell content area. 0 is interpreted as 65536
	 * 0x07: number of fragmented free bytes within the cell content area
	 * 0x08-0x0B: right-most pointer. This value appears in the header of interior b-tree pages only and is omitted from all other pages.
	 */
	public void writePageHeader() throws Exception{
		// apply only this.p points to the root page of the file (second page)
		writePageHeader(this.p);
	}
	
	
	/*
	 * Page Header: immediately follow the database header
	 * 0x00: one-byte flag: 0x02 - page is an interior index b-tree page; 0x05 - page is an interior table b-tree page; 0x0a - page is a leaf index b-tree page; 0x0d - page is a leaf table b-tree page
	 * 0x01-0x02: start of the first free block on the page or 0 if there is no free blocks
	 * 0x03-0x04: number of cells on the page
	 * 0x05-0x06: start of the cell content area. 0 is interpreted as 65536
	 * 0x07: number of fragmented free bytes within the cell content area
	 * 0x08-0x0B: right-most pointer. This value appears in the header of interior b-tree pages only and is omitted from all other pages.
	 */
	public void writePageHeader(Page p) throws Exception{
		recalFilePages();
		boolean leaf = p.leaf;
		boolean table = this.table;
		boolean root = p.schemaRoot;
		
		
		// if the page is database page(root page), we can directly write following database header
		// if the page is not database page(root page), we write according to the type of the page
		// if it is leaf, page header should contain only 8 bytes otherwise 12 bytes.
		
		if (table){	
			// table file
			if (root){
				// seek file pointer
				this.rfile.seek(0x44);
				// write flag, root page is always treated as a leaf table b-tree page
				this.rfile.writeByte(13);		
			}
			if(leaf && !root){
				this.rfile.seek(p.pagNumber*FileHandler.pageSize);
				// write flag
				this.rfile.writeByte(13);
				
			}
			if(!leaf && !root){
				this.rfile.seek(p.pagNumber*FileHandler.pageSize);
				// write flag, interior table b-tree page
				this.rfile.writeByte(5);
			}
			
			// write first free block 
			this.rfile.writeShort(p.freeBlockStart);
			
			// write number of cells on the page
			this.rfile.writeShort(p.cellNum);
			
			// write start of the cell content area
			this.rfile.writeShort(p.cellStart);
			
			// write number of fragmented free bytes within the cell content area
			this.rfile.writeByte(p.fragNum);
			
			// reserve pointer space for both leaf and non-leaf nodes
			if (!root){
				// write right pointer
				this.rfile.writeInt(p.rightPointer);
			}
		}
		else{	
			// index file
			if (root){
				// seek file pointer
				this.rfile.seek(0x44);
				// write flag, root page is always treated as a leaf table b-tree page
				this.rfile.writeByte(13);
				
			}
			if(leaf && !root){
				this.rfile.seek(p.pagNumber*FileHandler.pageSize);
				// write flag
				this.rfile.writeByte(10);
				
			}
			if(!leaf && !root){
				this.rfile.seek(p.pagNumber*FileHandler.pageSize);
				// write flag, interior table b-tree page
				this.rfile.writeByte(2);
			}
			
			// write first free block 
			this.rfile.writeShort(p.freeBlockStart);
			
			// write number of cells on the page
			this.rfile.writeShort(p.cellNum);
			
			// write start of the cell content area
			this.rfile.writeShort(p.cellStart);
			
			// write number of fragmented free bytes within the cell content area
			this.rfile.writeByte(p.fragNum);
			
			// reserve space for both leaf and non-leaf nodes
			if (!root){
				// write right pointer
				this.rfile.writeInt(p.rightPointer);
			}
		}	
	}
	/*
	 * store the davisbase_tables, davisbase_columns headers in the root page
	 */
	/* davisbase_table headers*/
	static String[] davisbase_table_header = new String[]{
			"TABLE_CATALOG",
			"TABLE_SCHEMA",
			"TABLE_NAME",
			"TABLE_TYPE",
			"ENGINE",
			"ROW_FORMAT",
			"TABLE_ROWS",
			"MAX_RECORD_LENGTH",
			"CREATE_TIME",
			"UPDATE_TIME",
			"CHECK_TIME"
	};
	
	static String[] davisbase_column_header = new String[]{
			"TABLE_CATALOG",
			"TABLE_SCHEMA",
			"TABLE_NAME",
			"COLUMN_NAME",
			"COLUMN_DEFAULT",
			"IS_NULLABLE",
			"DATA_TYPE",
			"CHAR_MAX_LENGTH",
			"NUMERIC_PREC",
			"NUMERIC_SCAL",
			"CHAR_SET_NAME",
			"COLUMN_TYPE",
			"COLUMN_KEY"
	};
	
	
	public void writeSchemaCols(String[] schema_cols) throws Exception{
		// cell format:
		// payload_length(2 bytes)|colId(4 bytes)|payload
		int payloadLength = 0;
		int colId = 0;
		int offset = p.cellStart;
		int numOfCells = 0;
		
		// write cell pointer, start address at 0x4C
		int cellPointerStart = 0x4C;
		
		for (int idx=0;idx<schema_cols.length;idx++){
			payloadLength = schema_cols[idx].length();
			offset = offset - (2+4+payloadLength);
			
			
			this.rfile.seek(offset);
			
			// write payload_length
			this.rfile.writeShort(payloadLength);
			
			// write colId
			this.rfile.writeInt(colId);
			
			// write payLoad
			this.rfile.writeBytes(schema_cols[idx]);
			
			this.rfile.seek(cellPointerStart);
			this.rfile.writeShort(offset);
			
			// move file pointer back
			this.rfile.seek(offset);
			
			// update counters,etc
			colId++;
			cellPointerStart += 2;
			numOfCells++;
		}
		updateSchemaPage0Header(numOfCells,this.schemaRoot);
		onSchemaFileCreated();
	}
	
	public void updateSchemaPage0Header(int numOfCells, Page p) throws Exception{
		// write number of cells
		this.rfile.seek(0x47);
		
		p.cellNum = numOfCells;
		
		// scan for free block start address
		int[] result = scanForFBlockandFrag(p,this);
		int firstFreeBKPos = result[0];
		int numOfFrag = result[1];
		
		p.freeBlockStart = firstFreeBKPos;
		p.fragNum = numOfFrag;
		
		// remember to update leaf, right pointer
		// write page Header
		writePageHeader(p);
	}
	
	/* 
	 * Scan for unallocated space between last inserted cell and end of cell pointer array
	 * @param p page to scan
	 * @param handler file to scan
	 */
	private int scanForUnallocatedSpace(Page p,FileHandler handler) throws IOException{
		RandomAccessFile rfile = handler.getRFile();
		int numOfCells = 0;
		long start = 0;
		if (p.schemaRoot){
			start = 0x004C;
			rfile.seek(0x0047);
			numOfCells = rfile.readShort();
		}
		else{
			start = p.pagNumber*FileHandler.pageSize+0x000C;
			rfile.seek(p.pagNumber*FileHandler.pageSize+0x0003);
			numOfCells = rfile.readShort();
		}
		
		if(numOfCells == 0)
			return FileHandler.pageSize-12-this.uspace; // pageSize - (pageHeader size + reserved space)
		// last cell pointer position
		long lastCellPointer = start+(numOfCells)*2;
		// last cell pointer value
		int lastCell = scanForLastCellPointer(p,handler);
		
		// number of available bytes
		return (int) (lastCell-(lastCellPointer-p.pagNumber*FileHandler.pageSize));
	}
	
	/*
	 * find the end of the page header
	 * @param p page to scan
	 * @return the binary file address of page header
	 */
	private long scanForPageHeaderEnd(Page p) throws IOException{
		long end = 0;
		if (p.schemaRoot){
			end = 0x004C;
		}
		else{
			end = p.pagNumber*FileHandler.pageSize+0x000C;
		}
		return end;
	}
	
	/*
	 * return cell pointer values
	 * @param p page to scan
	 * @param handler file to process 
	 */
	private int[] scanForCellPointers(Page p, FileHandler handler) throws IOException{

		RandomAccessFile rfile = handler.getRFile();
		ArrayList<Integer> pointers = new ArrayList<>();
		int numOfCells = 0;
		long start = 0;
		if(p.schemaRoot){
			start = 0x004C;
			rfile.seek(0x0047);
			numOfCells = rfile.readShort();
		}
		else{
			start = p.pagNumber*FileHandler.pageSize+0x000C;

			rfile.seek(p.pagNumber*FileHandler.pageSize+0x0003);
			numOfCells = rfile.readShort();
		}
		
		rfile.seek(start);
		int pointer = -1;
		for (int i=0;i<numOfCells;i++){
			pointer = rfile.readShort();
			pointers.add(pointer);
			start+=2;
		}
		return Utilities.toIntArray(pointers.toArray(new Integer[]{}));
	}
	
	
	/* return the last inserted cell position
	 * @param p page to scan 
	 * @param handler file to process
	 */
	private int scanForLastCellPointer(Page p,FileHandler handler) throws IOException{
		int[] pointers = scanForCellPointers(p, handler);
		int min = pointers[0];
		for (int i=1;i<pointers.length;i++){
			if(min>pointers[i])
				min = pointers[i];
		}
		return min;
	}
	
	/*
	 * note it should be called before alter pointer arrays
	 */
	public int[] scanForFBlockandFrag(Page p,FileHandler handler) throws IOException{
		RandomAccessFile rfile = handler.getRFile();
		
		int[] result = new int[2];
		int[] pointers = scanForCellPointers(p,handler);
		long baseAddress = p.pagNumber*FileHandler.pageSize;
		Arrays.sort(pointers);
		
		// start of the free space
		long freeStart = 0;
				
		// length of free region
		int length = 0;
		int tmp_length = 0;
		
		
		// last freeBlock
		long lastFreeBKPos = 0;
		
		// first freeBlock
		long firstFreeBKPos = 0;
		
		// free block counter 
		int numOfFreeBK = 0;
		
		// number of fragmented bytes
		int numOfFrag = 0;
		
		// adjacent deleteMarker
		byte adjDeleteMarker = 0;
		
		
		// scan the cell content area
		// complete record format: deleteMarder | record format
		for(int i=0;i<pointers.length;i++){
			long pos = baseAddress + pointers[i];
			
			rfile.seek(pos);
			byte deleteMarker = rfile.readByte();
			
			if(deleteMarker == 1){
				rfile.seek(pos+1);
				tmp_length = rfile.readShort()+2+4+1;
			}
			
			if(adjDeleteMarker == 0 && deleteMarker==1){
				length = 0;
				freeStart = pos;
				length += tmp_length;
				if (i==pointers.length-1){
					firstFreeBKPos = freeStart;
					rfile.seek(firstFreeBKPos+2);
					rfile.writeShort(length);
					lastFreeBKPos = freeStart;
				}
			}
			if (adjDeleteMarker == 1 && deleteMarker == 0){
				if(length > 4){
					if(numOfFreeBK == 0){
						firstFreeBKPos = freeStart;
						rfile.seek(firstFreeBKPos+2);
						rfile.writeShort(length);
						lastFreeBKPos = freeStart;
						
					}
					else{
						rfile.seek(lastFreeBKPos);
						rfile.writeShort((int)(freeStart-baseAddress));
						rfile.seek(freeStart+2);
						rfile.writeShort(length);
						lastFreeBKPos = freeStart;
						
					}
					numOfFreeBK++;
				}
				else{
					numOfFrag += length;
				}
			}
			if (deleteMarker == 1 && adjDeleteMarker == 1){
				rfile.seek(pos+1);
				length+=(rfile.readShort()+2+4+1);	
				if (i==pointers.length-1){
					firstFreeBKPos = freeStart;
					rfile.seek(firstFreeBKPos+2);
					rfile.writeShort(length);
					lastFreeBKPos = freeStart;
				}
			}
			adjDeleteMarker = deleteMarker;
			
		}
		
		// set the last freeBlock address part = 0
		if(lastFreeBKPos != 0){
			rfile.seek(lastFreeBKPos);
			rfile.writeShort(0);
		}
		
		if(firstFreeBKPos!=0)
			result[0] = (int) (firstFreeBKPos - baseAddress);
		else
			result[0] = (int) firstFreeBKPos;
		result[1] = numOfFrag;
		return result; 
	}
	
	/*
	 * 	update or delete records in a table
	 *  by default, this -> file to process
	 *  @param typeCodes type code array of updated data
	 *  @param values values of the updated data
	 *  @param schema if the table is a schema table, i.e. davisbase_tables, davisbase_columns
	 *  @param tableName the name of the table
	 *  @param indexes index of the column to modify
	 *  @param handler file to process
	 *  @param delete delete records
	 *  @param update update records
	 *  @param operator operator of the where condition
	 *  @param value value in where condition
	 *  @param operIndex index of the column in where condition
	 *  @param where there is a where condition or not
	 *  
	 *  where== false: operator,value operIndex = null
	 *  delete indexes, typeCodes, values=null
	 */
	public void updateDelRecs(int[] indexes, HashMap<Integer, Any> values, boolean schema,String tableName,
			FileHandler handler,boolean delete, boolean update, boolean drop,String operator, Any value, int operIndex, boolean priInt,boolean primary,
		     boolean where,HashMap<Integer,Integer> typeCodeList, String database) throws Exception{
		
		if(this.databasePages == 1)
			throw new Exception(String.format("Error 14: table {} contains no data!",tableName));
		if(this.root == null)
			this.root = getRoot();
		// find the start page
		// pages form a linked list
		Page startP;
		int endPNum = 0;
		
		// if it is schema table, row id is primary key, brute force search all page
		if(schema){
			int pagNum = traversePage(null,null,handler.getRoot(),schema,true,true,handler,tableName);
			startP = preparePage(pagNum,handler);
		}
		else{
			if(!where){
				int pagNum = traversePage(null,null,handler.getRoot(),schema,true,true,handler,tableName);
				startP = preparePage(pagNum,handler);
			}
			else{
				if(operator.equals(">")||operator.equals(">=")){
					if((primary && !priInt) || !primary){
						int pagNum = traversePage(null,null,handler.getRoot(),schema,true,true,handler,tableName);
						startP = preparePage(pagNum,handler);
					}
					else{
						int pagNum = traversePage2(value.extract_long(),handler.getRoot(),schema,false,false,handler,tableName);
						startP = preparePage(pagNum,handler);
					}
				}
				else if(operator.equals("<")||operator.equals("<=")){
					if((primary && !priInt) || !primary){
						int pagNum = traversePage(null,null,handler.getRoot(),schema,true,true,handler,tableName);
						startP = preparePage(pagNum,handler);
					}
					else{
						endPNum = traversePage2(value.extract_long(),handler.getRoot(),schema,false,false,handler,tableName);
						if(endPNum!=0)
							endPNum = getLeafPagePointer(preparePage(endPNum,handler),handler.getRFile());
						if(endPNum!=0)
							endPNum = getLeafPagePointer(preparePage(endPNum,handler),handler.getRFile());

						int pagNum;
						pagNum = traversePage(null,null,handler.getRoot(),schema,true,true,handler,tableName);
						startP = preparePage(pagNum,handler);
					}
				}
				else if(operator.equals("!=") || operator.equals("<>")){
					int pagNum = traversePage(null,null,handler.getRoot(),schema,true,true,handler,tableName);
					startP = preparePage(pagNum,handler);
				}
				else{
					if((primary && !priInt) || !primary){
						int pagNum = traversePage(null,null,handler.getRoot(),schema,true,true,handler,tableName);
						startP = preparePage(pagNum,handler);
					}
					else{
						int pagNum = traversePage2(value.extract_long(),handler.getRoot(),schema,false,false,handler,tableName);	
						startP = preparePage(pagNum,handler);
					}
				}
			}
		}
		if(update)
			updateRecord(startP,endPNum,schema,indexes,values,operIndex,handler,value,operator,tableName,priInt,primary,where,typeCodeList);
		if(delete)
			deleteRecord(startP,endPNum,schema,operIndex,handler,value,operator,tableName,priInt,primary);
		if(drop)
			dropTable(startP,endPNum,schema,tableName,handler,database);
	}
	 
	/*
	 * drop information of table with name - tableName in two schema tables
	 */
	private void dropTable(Page startP, int endPNum, boolean schema, String tableName, FileHandler handler, String database) throws Exception {

		RandomAccessFile rfile = handler.getRFile();
		int nextLeafPage; // next leaf page pointer = 0 -> null
		Page p=startP;
		do{
			nextLeafPage = getLeafPagePointer(p,handler.getRFile());
			if(p.cellNum!=0){
				Long[] recordAddress = getPageRecords(handler,p,-1,null,null,schema,tableName,false,false,true,database);
				
				int[] cellPointers = scanForCellPointers(p,handler);
				ArrayList<Integer> pointers = Utilities.toArrayList(cellPointers);
				for(int i=0;i<recordAddress.length;i++){
					rfile.seek(recordAddress[i]);
					rfile.writeByte(1);
					pointers.remove(new Integer((int) (recordAddress[i]-p.pagNumber*FileHandler.pageSize)));
				}
				if(!pointers.isEmpty()){
					cellPointers = Utilities.toIntArray(pointers.toArray(new Integer[]{}));
					p.cellNum = cellPointers.length;
				}
				else
					p.cellNum = 0;
				int[] result = scanForFBlockandFrag(p,handler);
				p.freeBlockStart = result[0];
				p.fragNum = result[1];
				handler.writePageHeader(p);
			}
			p = preparePage(nextLeafPage,handler);
		}while(nextLeafPage!=endPNum);
	}	
	

	/*
	 * 	select records in a table
	 *  by default, this -> file to process
	 *  @param schema if the table is a schema table, i.e. davisbase_tables, davisbase_columns
	 *  @param tableName the name of the table
	 *  @param handler file to process
	 *  @param operator operator of the where condition
	 *  @param value value in where condition
	 *  @param operIndex index of the column in where condition
	 *  @param where there is a where condition or not
	 *  
	 *  where== false: operator,value operIndex = null
	 */
	public void selectRecs(boolean schema,String tableName, FileHandler handler, String operator, Any value, int operIndex, boolean priInt,boolean primary,String[] ColNames, 
		     boolean where, ArrayList<HashMap<String,Any>> rows, ArrayList<HashMap<String,Integer>> typeCodes) throws Exception{
		
		if(this.databasePages == 1)
			throw new Exception(String.format("Error 14: table {} contains no data!",tableName));
		if(this.root == null)
			this.root = getRoot();
		// find the start page
		// pages form a linked list
		Page startP;
		int endPNum = 0;
		
		// if it is schema table, row id is primary key, brute force search all page
		if(schema){
			int pagNum = traversePage(null,null,handler.getRoot(),schema,true,true,handler,tableName);
			startP = preparePage(pagNum,handler);
		}
		else{
			if(!where){
				int pagNum = traversePage(null,null,handler.getRoot(),schema,true,true,handler,tableName);
				startP = preparePage(pagNum,handler);
			}
			else{
				if(operator.equals(">")||operator.equals(">=")){
					if((primary && !priInt) || !primary){
						int pagNum = traversePage(null,null,handler.getRoot(),schema,true,true,handler,tableName);
						startP = preparePage(pagNum,handler);
					}
					else{
						int pagNum = traversePage2(value.extract_long(),handler.getRoot(),schema,false,false,handler,tableName);
						startP = preparePage(pagNum,handler);
					}
				}
				else if(operator.equals("<")||operator.equals("<=")){
					if((primary && !priInt) || !primary){
						int pagNum = traversePage(null,null,handler.getRoot(),schema,true,true,handler,tableName);
						startP = preparePage(pagNum,handler);
					}
					else{
						endPNum = traversePage2(value.extract_long(),handler.getRoot(),schema,false,false,handler,tableName);
						endPNum = getLeafPagePointer(preparePage(endPNum,handler),handler.getRFile());
						int pagNum;
						pagNum = traversePage(null,null,handler.getRoot(),schema,true,true,handler,tableName);
						startP = preparePage(pagNum,handler);
					}
				}
				else if(operator.equals("!=") || operator.equals("<>")){
					int pagNum = traversePage(null,null,handler.getRoot(),schema,true,true,handler,tableName);
					startP = preparePage(pagNum,handler);
				}
				else{
					if((primary && !priInt) || !primary){
						int pagNum = traversePage(null,null,handler.getRoot(),schema,true,true,handler,tableName);
						startP = preparePage(pagNum,handler);
					}
					else{
						int pagNum = traversePage2(value.extract_long(),handler.getRoot(),schema,false,false,handler,tableName);	
						startP = preparePage(pagNum,handler);
					}
				}
			}
		}
		selectRecord(startP, endPNum, schema, operIndex, handler, value, operator, tableName,priInt,  primary, rows,typeCodes, ColNames,where);
	}
	
	private void selectRecord(Page startP, int endPNum, boolean schema, int operIndex, FileHandler handler, Any value,
			String operator, String tableName, boolean priInt, boolean primary, ArrayList<HashMap<String, Any>> rows, ArrayList<HashMap<String, Integer>> typeCodes, String[] colNames,boolean where) throws Exception {

			RandomAccessFile rfile = handler.getRFile();
			org.omg.CORBA.ORB orb = ORB.init();
			int nextLeafPage; // next leaf page pointer = 0 -> null
			Page p=startP;
			do{
				nextLeafPage = getLeafPagePointer(p,handler.getRFile());
				if(p.cellNum!=0){
					Long[] recordAddress = getPageRecords(handler,p,operIndex,operator,value,schema,tableName,priInt,primary,where);


					for(int i=0;i<recordAddress.length;i++){
						rfile.seek(recordAddress[i]+1+2+4);
						
						int payloadHeaderSize = rfile.readShort();
						
						HashMap<String,Integer> map = new HashMap<>();
						for(int idx=0;idx<payloadHeaderSize-2;idx++){
							map.put(colNames[idx], (int) rfile.readByte());

						}

						typeCodes.add(map);
						
						HashMap<String,Any> map2 = new HashMap<>();
						for(int idx=0;idx<map.size();idx++){
							int code = map.get(colNames[idx]);
							Any any = orb.create_any();
							readValue(any,code,rfile);
							map2.put(colNames[idx], any);

						}
						rows.add(map2);
					}
				}
				p = preparePage(nextLeafPage,handler);
			}while(nextLeafPage!=endPNum);
	}

	private void deleteRecord(Page startP, int endPNum, boolean schema, int operIndex, FileHandler handler, Any value,
			String operator, String tableName,boolean priInt, boolean primary) {
		System.out.println("Warning: command currently not supported. It is in the support plan in future versions.");
	}

	private void updateRecord(Page startP, int endPNum, boolean schema, int[] indexes,
			HashMap<Integer, Any> values, int operIndex, FileHandler handler, Any value, String operator,
			String tableName,boolean priInt,boolean primary,boolean where,HashMap<Integer,Integer> typeCodeList) throws Exception {

		RandomAccessFile rfile = handler.getRFile();
		org.omg.CORBA.ORB orb = ORB.init();
		int nextLeafPage; // next leaf page pointer = 0 -> null
		Page p=startP;
		int[] nullCodeCollection = new int[]{0x00,0x01,0x02,0x03};
		ArrayList<Integer[]> allTypeCodesList = new ArrayList<>();
		ArrayList<HashMap<Integer,Any>> allValueList = new ArrayList<>();
		
		do{
			nextLeafPage = getLeafPagePointer(p,handler.getRFile());

			if(p.cellNum!=0){
				Long[] recordAddress = getPageRecords(handler,p,operIndex,operator,value,schema,tableName,priInt,primary,where);
				
				for(int i=0;i<recordAddress.length;i++){
					boolean rewrite=false;
					for(int j=0;j<indexes.length;j++){
						rfile.seek(recordAddress[i]+1+2+4+2+indexes[j]);
						int colTypeCode = rfile.readByte();
						Any colValue = values.get(indexes[j]);
						if(colTypeCode>=0x0C){
							String tmp = colValue.extract_string();
							if(tmp.length()>(colTypeCode-0x0C))
								rewrite=true;
							break;
						}
					}

					if(rewrite){
						// construct the new record
						rfile.seek(recordAddress[i]+1+2+4);
						int payloadHeaderLength = rfile.readShort();
						int[] typeCodes = new int[payloadHeaderLength-2];
						int[] tmp_typeCodes = new int[payloadHeaderLength-2];
						rfile.seek(recordAddress[i]+1+2+4+2);	
						
						for(int idx=0;idx<typeCodes.length;idx++){
							if(Utilities.In(idx, indexes)){
								int tmp = rfile.readByte();
								tmp_typeCodes[idx] = tmp;
								if(tmp<0x0C){
									if(Utilities.In(tmp, nullCodeCollection)){
										tmp = typeCodeList.get(idx);
									}
									if(Utilities.In(typeCodeList.get(idx),nullCodeCollection)){
										tmp = typeCodeList.get(idx);
									}
									typeCodes[idx]=tmp;
								}
								else{
									Any colValue = values.get(idx);
									String newVal = colValue.extract_string();
									if(newVal.length()>(tmp-0x0C))
										typeCodes[idx]=0x0C+newVal.length();
									else
										typeCodes[idx]=tmp;
								}
							}
							else{
								typeCodes[idx] = rfile.readByte();
								tmp_typeCodes[idx] = typeCodes[idx];
							}
						}
							
						HashMap<Integer,Any> val = new HashMap<>();
						// read values
						for(int idx=0;idx<tmp_typeCodes.length;idx++){
							Any any = orb.create_any();
							if(Utilities.In(idx,indexes)){
								val.put(idx, values.get(idx));
								readValue(any,tmp_typeCodes[idx],rfile);
							}
							else{
								readValue(any,tmp_typeCodes[idx],rfile);
								val.put(idx, any);
							}
						}
							
						//delete the record
						rfile.seek(recordAddress[i]);
						rfile.writeByte(1);
						int[] cellPointers_tmp = scanForCellPointers(p,handler);
						int[] cellPointers = new int[cellPointers_tmp.length-1];
						int j=0;
						for(int idx=0;idx<cellPointers_tmp.length;idx++)
							if(cellPointers_tmp[idx]!=(recordAddress[i]-p.pagNumber*FileHandler.pageSize))
								cellPointers[j++] = cellPointers_tmp[idx];
						int[] result = scanForFBlockandFrag(p,handler);
						
						p.freeBlockStart=result[0];
						p.fragNum = result[1];
						p.cellNum--;
						handler.writePageHeader(p);

						long pageHeaderEnd = scanForPageHeaderEnd(p);
						rfile.seek(pageHeaderEnd);
						for(int idx=0;idx<cellPointers.length;idx++)
							rfile.writeShort(cellPointers[idx]);
							
						// store the new record
						allTypeCodesList.add(Utilities.toIntegerArray(typeCodes));
						allValueList.add(val);
					}
					else{
						for(int idx=0;idx<indexes.length;idx++){
							// read column type code
							rfile.seek(recordAddress[i]+1+2+4+2+indexes[idx]);
							int colTypeCode = rfile.readByte();
							if(Utilities.In(colTypeCode, nullCodeCollection)){
								rfile.seek(recordAddress[i]+1+2+4+2+indexes[idx]);
								colTypeCode = typeCodeList.get(indexes[idx]);
								rfile.writeByte(colTypeCode);
							}

							if(Utilities.In(typeCodeList.get(indexes[idx]),nullCodeCollection)){
								rfile.seek(recordAddress[i]+1+2+4+2+indexes[idx]);
								colTypeCode = typeCodeList.get(indexes[idx]);
								rfile.writeByte(colTypeCode);
							}
							Any colValue = values.get(indexes[idx]);
							
							// move to the correct position
							int length = 0;
							int payloadHeaderSize=0;
							rfile.seek(recordAddress[i]+1+2+4);
							payloadHeaderSize = rfile.readShort();
							for(int j=0;j<indexes[idx];j++)
								length+= Utilities.getColumnSize(rfile.readByte());
							rfile.seek(recordAddress[i]+1+2+4+payloadHeaderSize+length);
							writeValue(colTypeCode,colValue,rfile);
						}
					}
				}
			}
			p = preparePage(nextLeafPage,handler);
		}while(nextLeafPage!=endPNum);
		if(!allTypeCodesList.isEmpty() && !allValueList.isEmpty()){
			for(int idx=0;idx<allTypeCodesList.size();idx++){
				int[] typeCodes = Utilities.toIntArray(allTypeCodesList.get(idx));
				HashMap<Integer,Any> val = allValueList.get(idx);
				handler.writeTableRow(typeCodes, val, schema, tableName);
			}
		}
	}
	private Long[] getPageRecords(FileHandler handler, Page p, int operIndex, String operator, Any value,
			boolean schema, String tableName,boolean priInt, boolean primary,boolean where) throws Exception {
		return getPageRecords(handler,p,operIndex,operator,value,schema,tableName,priInt,primary,where,null);
	}

	private Long[] getPageRecords(FileHandler handler, Page p, int operIndex, String operator, Any value,
			boolean schema, String tableName,boolean priInt, boolean primary,boolean where,String database) throws Exception {

		ArrayList<Long> recAddress = new ArrayList<>();
		RandomAccessFile rfile = handler.getRFile();
		if(p.cellNum == 0)
			return null;
		long baseAddress = p.pagNumber*FileHandler.pageSize;
		int[] pointers = scanForCellPointers(p,handler);
		org.omg.CORBA.ORB orb = ORB.init();
				
		for(int i=0;i<pointers.length;i++){
			
			
			long address = baseAddress + pointers[i];
			
			Any columnRec = orb.create_any();
			int typeCode_compare;
			
			// read compare column
			if(where){
				if(schema || (primary && priInt)){
					rfile.seek(address+3);
					int rec_key = rfile.readInt();
					columnRec.insert_long(rec_key);
					typeCode_compare = 0x06;
				}
				else{
					// column is not primary column or it is primary column but is not integer
				
					// read header length
					int valueLength = 0;
					rfile.seek(address+1+2+4);
					valueLength = rfile.readShort();
				
					// read value record length preceding 
					int length = 0;
					for(int idx=0;idx<operIndex;idx++){
						rfile.seek(address+1+2+4+2+idx);
						int tmp = rfile.readByte();
						length+=Utilities.getColumnSize(tmp);
					}
					rfile.seek(address+1+2+4+2+operIndex);
					typeCode_compare = rfile.readByte();
					
					// read the column
					rfile.seek(address+1+2+4+valueLength+length);
					columnRec = orb.create_any();
					readValue(columnRec,typeCode_compare,rfile);	
				}
				if(!schema){
					// filter tuples based on compare column
					// columnRec operator value
					if(!checkCondition(columnRec,value,operator,typeCode_compare)){
						continue;
					}
					else{
						recAddress.add(address);	
					}
				}
				else{
					// read payload length
					rfile.seek(address+1+2+4);
					int payloadHeaderSize = rfile.readShort();
				
					// read table name 
					int length = 0;
					int length2 = 0;
					rfile.seek(address+9);				
					length += (rfile.readByte()-0x0C);
					length2 = length;
					byte typeCode2 = rfile.readByte();
					length += (typeCode2-0x0C);
					byte typeCode1 = rfile.readByte();
					rfile.seek(address+1+2+4+payloadHeaderSize+length);
					Any tmp = orb.create_any();
					readValue(tmp,typeCode1,rfile);
					String rec_tableName = tmp.extract_string().trim();
					
					rfile.seek(address+1+2+4+payloadHeaderSize+length2);
					tmp = orb.create_any();
					readValue(tmp,typeCode2,rfile);
					String rec_schemaName = tmp.extract_string().trim();
				
					if(!rec_schemaName.equals(database) || !((tableName.trim()).equals(rec_tableName)))
						continue;
					else{
						recAddress.add(address);
					}
				}
			}
			else{
				recAddress.add(address);
			}
		}
		return recAddress.toArray(new Long[]{});
	}

	private boolean checkCondition(Any columnRec, Any value, String operator, int typeCode_compare) throws Exception {
		switch(typeCode_compare){
		case 0x04:
			byte rec_key=columnRec.extract_octet();
			byte key = value.extract_octet();
			return Utilities.compare(rec_key, key, operator);			
		case 0x05:
			short rec_key1=columnRec.extract_short();
			short key1=value.extract_short();
			return Utilities.compare(rec_key1, key1, operator);
		case 0x06:
			int rec_key2=columnRec.extract_long();
			int key2 = value.extract_long();
			return Utilities.compare(rec_key2, key2, operator);
		case 0x07:
			long rec_key3=columnRec.extract_longlong();
			long key3 = value.extract_longlong();
			return Utilities.compare(rec_key3, key3, operator);
		case 0x08:
			float rec_key4=columnRec.extract_float();
			float key4 = value.extract_float();
			return Utilities.compare(rec_key4, key4, operator);
		case 0x09:
			double rec_key5=columnRec.extract_double();
			double key5 = value.extract_double();
			return Utilities.compare(rec_key5, key5, operator);
		case 0x0A:
			long rec_key6=columnRec.extract_longlong();
			long key6 = value.extract_longlong();
			return Utilities.compare(rec_key6, key6, operator);
		case 0x0B:
			long rec_key7=columnRec.extract_longlong();
			long key7 = value.extract_longlong();
			return Utilities.compare(rec_key7, key7, operator);
		default:
			if(typeCode_compare>=0x0C){
				String rec_key8=columnRec.extract_string().trim();
				String key8 = value.extract_string().trim();
				return Utilities.compare(rec_key8, key8, operator);
			}
		}
		return false;
	}

	/*
	 *  write a row for a table
	 *  by default, this -> file to process
	 *  @param typeCodes type code array of the new data
	 *  @param values values of the new data
	 *  @param schema if the table is a schema table, i.e. davisbase_tables, davisbase_columns
	 *  @param tableName the name of the table
	 */
	public void writeTableRow(int[] typeCodes, HashMap<Integer, Any> values, boolean schema,String tableName) throws Exception{
		
		
		if(this.databasePages == 1){
			// file contains only database page
			initializePage(this.databasePages);
			writePageHeader(this.p);
		}
		if(this.root == null)
			this.root = getRoot();
		writeTableRow(typeCodes,values,this.root, schema,tableName);
	}
	
	/*
	 * write a table row
	 * by default, this -> file to write
	 * @param typeCodes type code array of the new data
	 * @param values values of the new data
	 * @param p the table page on which the new data is written
	 * @param schema if the table is a schema table 
	 * @param tableName the name of the table
	 */
	public void writeTableRow(int[] typeCodes, HashMap<Integer, Any> values, Page p, boolean schema,String tableName) throws Exception {
		// insert this record into the B-tree
		
		if (p.leaf){
			int availSpace = scanForUnallocatedSpace(p,this);
			int recordLength = Utilities.calRecordLength(typeCodes, p.leaf);

			
			if(p.cellNum+1 >= FileHandler.degree){
		
		
				// split the tree and get the root
				split(typeCodes,values,p,this,schema,tableName);
				// update the root
				this.root = this.getRoot();
				this.fileChangeCounter++;
				recalFilePages();
				writeDatabaseHeader(this.rfile);	
				if(!schema)
					updateDavisbaseTable(tableName);
			}
			else if(availSpace<recordLength && p.cellNum+1 < FileHandler.degree){
		
				// wipe and rewrite the page
				int[] cellPointers = scanForCellPointers(p,this);
				long baseAddress = p.pagNumber*FileHandler.pageSize;
				
				ArrayList<byte[]> records = new ArrayList<>();
				for(int i=0;i<cellPointers.length;i++){
					byte[] tmp = readRecord(cellPointers[i],baseAddress,p.leaf,this.rfile);
					records.add(tmp);
				}
				
				// move all records to end of the page
				long pageEndPos = (p.pagNumber+1)*FileHandler.pageSize-this.uspace;
				for(int i=0;i<records.size();i++){
					byte[] tmp = records.get(i);
					pageEndPos -= tmp.length;
					this.rfile.seek(pageEndPos);
					this.rfile.write(tmp);
				}	
				writeTableRow(typeCodes,values,p,schema,tableName);
			}
			else{

				if(p.cellNum>0){
					// remember to reorder the cell pointers
					int[] cellPointers = scanForCellPointers(p,this);
					int lastCellPointer = scanForLastCellPointer(p,this);
					long location = p.pagNumber*FileHandler.pageSize+lastCellPointer;
					location = location-recordLength;
					long start = scanForPageHeaderEnd(p);
					writeRecord(this,typeCodes,values,start,location,p,recordLength,schema,0,cellPointers,tableName);
				}
				else{
					long location = (p.pagNumber+1)*FileHandler.pageSize-this.uspace;
					location = location - recordLength;
					long start = scanForPageHeaderEnd(p);
					writeRecord(this,typeCodes,values,start,location,p,recordLength,schema,0,null,tableName);	
				}
				// update page header and database header
				p.cellNum++;
				int[] result = scanForFBlockandFrag(p,this);
				p.freeBlockStart = result[0];
				p.fragNum = result[1];
				this.fileChangeCounter++;
				recalFilePages();
				writePageHeader(p);
				writeDatabaseHeader(this.rfile);	
				if(!schema)
					updateDavisbaseTable(tableName);
			}
		}
		else{
			boolean bruteForce = false;
			if (schema)
				bruteForce = true;
			else{
				bruteForce = !checkPriInt(tableName);
			}
			Page page = traversePage1(typeCodes,values,p,schema,bruteForce,false,this,tableName);
			writeTableRow(typeCodes,values,page,schema,tableName);
		}	
		
	}
	
	private void updateDavisbaseTable(String tableName) throws Exception{
		FileHandler[] handler = openSchema();
		// define values for davisbase_tables.tbl
		int pagNum = traversePage(null,null,handler[0].getRoot(),true,true,true,handler[0],tableName);
		Page p = preparePage(pagNum,handler[0]);
		int nextLeafPage; // next leaf page pointer = 0 -> null
		
		HashMap<String,Any> record;
		ArrayList<HashMap<String,Any>> rows = new ArrayList<>();
		ArrayList<HashMap<String,Integer>> typeCodes = new ArrayList<>();
		ArrayList<Integer> keys = new ArrayList<>();
		int idx = -1;
		
		MainLoop:
		do{
			nextLeafPage = getLeafPagePointer(p,handler[0].getRFile());
			 rows = new ArrayList<>();
			 typeCodes = new ArrayList<>();
			 keys = new ArrayList<>();
			
			if(p.cellNum!=0)
				readPageRecords(handler[0],p,rows,typeCodes,keys,FileHandler.davisbase_table_header,-1,"=",true,tableName,true);
			for(int i=0;i<rows.size();i++){
				 record = rows.get(i);
				if(record.get("TABLE_NAME").extract_string().toLowerCase().trim().equals(tableName.toLowerCase().trim())){
					idx = i;
					break MainLoop;
				}
			}
			p = preparePage(nextLeafPage,handler[0]);
		}
		while(nextLeafPage!=0);
		
		if (idx == -1)
			throw new Exception(String.format("Error 08: %s table does not exist!",tableName));
		int[] pointers = scanForCellPointers(p,handler[0]);
		long baseAddress = p.pagNumber*FileHandler.pageSize;
		long address = baseAddress + pointers[idx];
		RandomAccessFile rfile = handler[0].getRFile();
		rfile.seek(address+1);
		
		// read number of bytes of payload
		int length = rfile.readShort();
		
		// move to table rows column
		rfile.seek(address+1+2+4+length-(8+8+8+4+4));
		
		// read table rows
		int rowNum = rfile.readInt();
		rfile.seek(rfile.getFilePointer()-4);
		rfile.writeInt(rowNum+1);
		
		// close
		handler[0].close();
		handler[1].close();
	}
	
	/*
	 *  Traverse the b+ -tree
	 *  @param typeCodes type code array of new data
	 *  @param values values of new data
	 *  @param p2 current page
	 *  @param schema if the table is a schema table or not
	 *  @param bruteForce traverse with brute force manner (key value is rowId)
	 *  @param reverse reverse= true go to left most child else right most child (used with brute force)
	 *  @param tableName the table name 
	 *  @param handler file to traverse
	 *  
	 *  typeCodes, values is not required if the table is a schema table or the key value is rowId
	 *  
	 */
	private int traversePage(int[] typeCodes, HashMap<Integer, Any> values, Page p2, boolean schema,boolean bruteForce,boolean reverse,FileHandler handler ,String tableName) throws Exception {

		
		RandomAccessFile rfile = handler.getRFile();
		// write table row - reverse = false always find the right most page
		// getTableRows - reverse = true always find the left most page
		// in a page, pointers are sorted based on key value (small -> large)
		
		if(schema || bruteForce)
		{
			// if there is no content on the page, simply return
			if(p2.cellNum == 0)
				return p2.pagNumber;
			
			// read pointers
			int[] pointers = scanForCellPointers(p2,handler);
			if(!p2.leaf){
				int targetPointer;
				if(reverse){
					targetPointer = pointers[0];
					long address = p2.pagNumber*FileHandler.pageSize+targetPointer;
					rfile.seek(address+1);
				}
				else{
					rfile.seek(p2.pagNumber*FileHandler.pageSize+0x08);			
				}
				
				int pageNumber = rfile.readInt();
				Page p = preparePage(pageNumber,handler);
				p.parentPage = p2;
				return traversePage(typeCodes,values,p,schema,bruteForce,reverse,handler,tableName);	
			}
			else{
				return p2.pagNumber;
			}
		}
		else{
			// normally traverse the b-tree according to key values
			int priKeyVal = getPriMaryKeyVal(typeCodes,values,p2,schema,tableName,handler);
			
			if (p2.cellNum == 0)
				return p2.pagNumber;
			
			if(p2.leaf)
				return p2.pagNumber;
			else{
				int[] pointers = scanForCellPointers(p2,handler);
				int[] pageKeyVals = getPageKeyVals(pointers,p2,handler);
				HashMap<Integer,Integer> key2idx = Utilities.KeyVal2Idx(pageKeyVals);
				Arrays.sort(pageKeyVals);
			
				int idx = Arrays.binarySearch(pageKeyVals, priKeyVal);
				
				// process the index -insertion_point-1
				if(idx < 0){
					idx = -idx-1;
				}
				
				if(idx == -1){
					//go to the left most child
					int keyVal = pageKeyVals[0];
					int i = key2idx.get(keyVal);
					long baseAddress = p2.pagNumber*FileHandler.pageSize;
					rfile.seek(pointers[i]+baseAddress+1);
					int leftMostChild = rfile.readInt();
					Page p = preparePage(leftMostChild,handler);
					p.parentPage = p2;
					return traversePage(typeCodes,values,p,schema,bruteForce,reverse,handler,tableName);
				}
				else if (idx == pageKeyVals.length){
					//go to the right most child, should read page header
					long baseAddress = p2.pagNumber*FileHandler.pageSize;
					long address = baseAddress + 0x08;
					rfile.seek(address);
					int rightMostChild = rfile.readInt();
					Page p = preparePage(rightMostChild,handler);
					p.parentPage = p2;
					return traversePage(typeCodes,values,p,schema,bruteForce,reverse,handler,tableName);
				}
				else{
					// read cell of this index
					int keyVal = pageKeyVals[idx];
					int i = key2idx.get(keyVal);
					long baseAddress = p2.pagNumber*FileHandler.pageSize;
					rfile.seek(baseAddress+pointers[i]+1);
					int targetPointer = rfile.readInt();
					Page p = preparePage(targetPointer,handler);
					p.parentPage = p2;
					return traversePage(typeCodes,values,p,schema,bruteForce,reverse,handler,tableName);
				}	
			}
		}
	}
	
	/*
	 *  Traverse the b+ -tree
	 *  @param key key to search
	 *  @param p2 current page
	 *  @param schema if the table is a schema table or not
	 *  @param bruteForce traverse with brute force manner (key value is rowId)
	 *  @param reverse reverse= true go to left most child else right most child (used with brute force)
	 *  @param tableName the table name 
	 *  @param handler file to traverse
	 *  
	 *  typeCodes, values is not required if the table is a schema table or the key value is rowId
	 *  
	 */
	private int traversePage2(int key, Page p2, boolean schema,boolean bruteForce,boolean reverse,FileHandler handler ,String tableName) throws Exception {
		
		RandomAccessFile rfile = handler.getRFile();
		// write table row - reverse = false always find the right most page
		// getTableRows - reverse = true always find the left most page
		// in a page, pointers are sorted based on key value (small -> large)
		
		if(schema || bruteForce)
		{
			// if there is no content on the page, simply return
			if(p2.cellNum == 0)
				return p2.pagNumber;
			
			// read pointers
			int[] pointers = scanForCellPointers(p2,handler);
			if(!p2.leaf){
				int targetPointer;
				if(reverse){
					targetPointer = pointers[0];
					long address = p2.pagNumber*FileHandler.pageSize+targetPointer;
					rfile.seek(address+1);
				}
				else{
					rfile.seek(p2.pagNumber*FileHandler.pageSize+0x08);			
				}
				
				int pageNumber = rfile.readInt();
				Page p = preparePage(pageNumber,handler);
				p.parentPage = p2;
				return traversePage2(key,p,schema,bruteForce,reverse,handler,tableName);	
			}
			else{
				return p2.pagNumber;
			}
		}
		else{
			// normally traverse the b-tree according to key values
			int priKeyVal = key;
			
			if (p2.cellNum == 0)
				return p2.pagNumber;
			
			if(p2.leaf)
				return p2.pagNumber;
			else{
				int[] pointers = scanForCellPointers(p2,handler);
				int[] pageKeyVals = getPageKeyVals(pointers,p2,handler);
				HashMap<Integer,Integer> key2idx = Utilities.KeyVal2Idx(pageKeyVals);
				Arrays.sort(pageKeyVals);
			
				int idx = Arrays.binarySearch(pageKeyVals, priKeyVal);
				
				// process the index -insertion_point-1
				if(idx < 0){
					idx = -idx-1;
				}
				
				if(idx == -1){
					//go to the left most child
					int keyVal = pageKeyVals[0];
					int i = key2idx.get(keyVal);
					long baseAddress = p2.pagNumber*FileHandler.pageSize;
					rfile.seek(pointers[i]+baseAddress+1);
					int leftMostChild = rfile.readInt();
					Page p = preparePage(leftMostChild,handler);
					p.parentPage = p2;
					return traversePage2(key,p,schema,bruteForce,reverse,handler,tableName);
				}
				else if (idx == pageKeyVals.length){
					//go to the right most child, should read page header
					long baseAddress = p2.pagNumber*FileHandler.pageSize;
					long address = baseAddress + 0x08;
					rfile.seek(address);
					int rightMostChild = rfile.readInt();
					Page p = preparePage(rightMostChild,handler);
					p.parentPage = p2;
					return traversePage2(key,p,schema,bruteForce,reverse,handler,tableName);
				}
				else{
					// read cell of this index
					int keyVal = pageKeyVals[idx];
					int i = key2idx.get(keyVal);
					long baseAddress = p2.pagNumber*FileHandler.pageSize;
					rfile.seek(baseAddress+pointers[i]+1);
					int targetPointer = rfile.readInt();
					Page p = preparePage(targetPointer,handler);
					p.parentPage = p2;
					return traversePage2(key,p,schema,bruteForce,reverse,handler,tableName);
				}	
			}
		}
	}
	/*
	 *  Traverse the b+ -tree
	 *  @param typeCodes type code array of new data
	 *  @param values values of new data
	 *  @param p2 current page
	 *  @param schema if the table is a schema table or not
	 *  @param bruteForce traverse with brute force manner (key value is rowId)
	 *  @param reverse reverse= true go to left most child else right most child (used with brute force)
	 *  @param tableName the table name 
	 *  @param handler file to traverse
	 *  
	 *  typeCodes, values is not required if the table is a schema table or the key value is rowId
	 *  
	 */
	private Page traversePage1(int[] typeCodes, HashMap<Integer, Any> values, Page p2, boolean schema,boolean bruteForce,boolean reverse,FileHandler handler ,String tableName) throws Exception {

		
		RandomAccessFile rfile = handler.getRFile();
		// write table row - reverse = false always find the right most page
		// getTableRows - reverse = true always find the left most page
		// in a page, pointers are sorted based on key value (small -> large)
		
		if(schema || bruteForce)
		{
			// if there is no content on the page, simply return
			if(p2.cellNum == 0)
				return p2;
			
			// read pointers
			int[] pointers = scanForCellPointers(p2,handler);
			if(!p2.leaf){
				int targetPointer;
				if(reverse){
					targetPointer = pointers[0];
					long address = p2.pagNumber*FileHandler.pageSize+targetPointer;
					rfile.seek(address+1);
				}
				else{
					rfile.seek(p2.pagNumber*FileHandler.pageSize+0x08);			
				}
				
				int pageNumber = rfile.readInt();
				Page p = preparePage(pageNumber,handler);
				p.parentPage = p2;
				return traversePage1(typeCodes,values,p,schema,bruteForce,reverse,handler,tableName);	
			}
			else{
				return p2;
			}
		}
		else{
			// normally traverse the b-tree according to key values
			int priKeyVal = getPriMaryKeyVal(typeCodes,values,p2,schema,tableName,handler);
			
			if (p2.cellNum == 0)
				return p2;
			
			if(p2.leaf)
				return p2;
			else{
				int[] pointers = scanForCellPointers(p2,handler);
				int[] pageKeyVals = getPageKeyVals(pointers,p2,handler);
				HashMap<Integer,Integer> key2idx = Utilities.KeyVal2Idx(pageKeyVals);
				Arrays.sort(pageKeyVals);
			
				int idx = Arrays.binarySearch(pageKeyVals, priKeyVal);


				// process the index -insertion_point-1
				if(idx < 0){
					idx = -idx-1;
				}
				if(idx == -1){
					//go to the left most child
					int keyVal = pageKeyVals[0];
					int i = key2idx.get(keyVal);
					long baseAddress = p2.pagNumber*FileHandler.pageSize;
					rfile.seek(pointers[i]+baseAddress+1);
					int leftMostChild = rfile.readInt();
					Page p = preparePage(leftMostChild,handler);
					p.parentPage = p2;
					return traversePage1(typeCodes,values,p,schema,bruteForce,reverse,handler,tableName);
				}
				else if (idx == pageKeyVals.length){
					//go to the right most child, should read page header
					long baseAddress = p2.pagNumber*FileHandler.pageSize;
					long address = baseAddress + 0x08;
					rfile.seek(address);
					int rightMostChild = rfile.readInt();
					Page p = preparePage(rightMostChild,handler);
					p.parentPage = p2;
					return traversePage1(typeCodes,values,p,schema,bruteForce,reverse,handler,tableName);
				}
				else{
					// read cell of this index
					int keyVal = pageKeyVals[idx];
					int i = key2idx.get(keyVal);
					long baseAddress = p2.pagNumber*FileHandler.pageSize;
					rfile.seek(baseAddress+pointers[i]+1);
					int targetPointer = rfile.readInt();
					Page p = preparePage(targetPointer,handler);
					p.parentPage = p2;
					return traversePage1(typeCodes,values,p,schema,bruteForce,reverse,handler,tableName);
				}	
			}
		}
	}

	/*
	 * get the primary key value
	 * typeCodes, values can be null if the table is a schema table or the key value is rowId
	 * @param typeCodes type code array of the new data
	 * @param values values of new data
	 * @param p2 currrent page
	 * @param schema if the table is a schema table or not
	 * @param tableName table name
	 * @return primary key value
	 */
	private int getPriMaryKeyVal(int[] typeCodes, HashMap<Integer, Any> values, Page p2, boolean schema, String tableName, FileHandler handler) throws Exception {
		RandomAccessFile rfile = handler.getRFile();
		if(schema){
			// there is no way to map an arbitrary string to an unique integer, use rowId as the key value,
			// rowId start from 1
			int pageNumber = traversePage(typeCodes,values,handler.getRoot(),schema,true,false,handler,tableName);

			Page p = preparePage(pageNumber,handler);
			int pointer;
			if(p.cellNum>0){
				int[] pointers = scanForCellPointers(p,handler);
				pointer = pointers[pointers.length-1];


				rfile.seek(p.pagNumber*FileHandler.pageSize+pointer+3);
				return rfile.readInt()+1;
			}
			else{
				// probably due to delete record
				// each page contains at most degree-1 records. Hence the max allowed rowId = (degree-1)*(database pages-1) rowId does not need to be continuous but should be unique
				if (p.parentPage != null){
					int pageNum = p.pagNumber;
					p = p.parentPage;
					int[] pointers = scanForCellPointers(p,handler);
					int[] pagePointVals = new int[pointers.length];
					int[] keys = new int[pointers.length];
					long baseAddress = p.pagNumber*FileHandler.pageSize;
					for(int i=0;i<pointers.length;i++){
						rfile.seek(baseAddress+pointers[i]+1);
						pagePointVals[i] = rfile.readInt();
						keys[i] = rfile.readInt();
					}
					pointer = -1;
					for(int i=0;i<pagePointVals.length;i++){
						if(pagePointVals[i] ==  pageNum){
							return keys[i-1]+1;
						}
					}
					return keys[pointers.length-1]+1; // return the last key value +1
				}
				else
					return p.cellNum+1;
			}
		}
		else{
			// refer to schema table and get the corresponding column list in the order when they are inserted.
			// read columns table and get all corresponding rows for the table
			ArrayList<HashMap<String, Any>> rows = new ArrayList<>();
			ArrayList<HashMap<String,Integer>> type = new ArrayList<>();
			ArrayList<Integer> keys = new ArrayList<>();
			FileHandler[] handler1 = openSchema();
			getTableRows(typeCodes,values,handler1[1],rows,type,keys,FileHandler.davisbase_column_header,-1,"=",true,true,tableName, true);
			
			handler1[0].close();
			handler1[1].close();
			
			// check which column is the primary key field
			int idx = checkPriColumn(rows);
			boolean priInt = false;
			if(idx != -1){
				HashMap<String,Any> priCol = rows.get(idx);
				priInt = checkPriInt(priCol);
				if (priInt){
					Any tmp = values.get(idx);
					int byteCode = typeCodes[idx];
					switch(byteCode){
					case 0x04:
						return (int)tmp.extract_octet();
					case 0x05:
						return (int)tmp.extract_short();
					case 0x06:
						return tmp.extract_long();
					}
				}
			}
			if (idx==-1 || !priInt){
				int pageNumber = traversePage(typeCodes,values,handler.getRoot(),schema,true,false,handler,tableName);
				Page p = preparePage(pageNumber,handler);
				int pointer;
				if(p.cellNum>0){
					int[] pointers = scanForCellPointers(p,handler);
					pointer = pointers[pointers.length-1];
					rfile.seek(p.pagNumber*FileHandler.pageSize+pointer+3);
					return rfile.readInt()+1;
				}
				else{
					// probably due to delete record
					// each page contains at most degree-1 records. Hence the max allowed rowId = (degree-1)*(database pages-1) rowId does not need to be continuous but should be unique
					
					if (p.parentPage != null){
						int pageNum = p.pagNumber;
						p = p.parentPage;
						int[] pointers = scanForCellPointers(p,handler);
						int[] pagePointVals = new int[pointers.length];
						int[] keys1 = new int[pointers.length];
						long baseAddress = p.pagNumber*FileHandler.pageSize;
						for(int i=0;i<pointers.length;i++){
							rfile.seek(baseAddress+pointers[i]+1);
							pagePointVals[i] = rfile.readInt();
							keys1[i] = rfile.readInt();
						}
						pointer = -1;
						for(int i=0;i<pagePointVals.length;i++){
							if(pagePointVals[i] ==  pageNum){
								return keys1[i-1]+1;
							}
						}
						return keys1[pointers.length-1]+1; // return the last key value +1
					}
					else
						return p.cellNum+1;
				}
			}
		}
		return -1;
	}
	
	private boolean checkPriInt(HashMap<String, Any> priCol) {
		Any any = priCol.get("DATA_TYPE");
	
		String dataType = any.extract_string().toLowerCase().trim();
		if(dataType.contains("int") && !dataType.contains("big"))
			return true;
		return false;
	}

	/* 
	 * return the column index which is the primary key, start from 0 
	 * return -1 if no column is marked as primary key
	 */
	public int checkPriColumn(ArrayList<HashMap<String,Any>> rows){
		HashMap<String,Any> map;
		for(int i=0;i<rows.size();i++){
			map = rows.get(i);
			if(map.get("COLUMN_KEY").extract_string().toLowerCase().trim().equals("pri"))
				return i;			
		}
		return -1;
	}

	/*
	 *  get all table rows
	 *  ArrayList<HashMap<String,Any>> an array of row records. HashMap is colName-value pair
	 *  ArrayList<Integer> an array of type code of each type for each column
	 *  key - the primary or candidate key value, must be an integer
	 *  filter - for normal table, filter = true if the search field is the primary key field (and is integer); false otherwise
	 *  if bruteForce = true, go to most left child
	 *  if schema=F and bruteForce=F normally traverse the B-tree
	 *  if schema=T, go to most left child
	 *  if schema=T, search based on tableName 
	 *  if schema=F, search based on key value -> if filter=T, filter records based on key value else take all records
	 *  
	 * @param handler file handler
	 * @param rows row container
	 * @param typeCodes typeCodes container
	 * @param keys keys container
	 * @param ColNames column name list of table
	 * @param k key value of row you want to find
	 * @param operator operation operator
	 * @param schema if the table is a schema table or not
	 * @param bruteforce traverse the b+ -tree in a brute force manner
	 * @param tableName name of the table
	 * @param fitler filter the rows according to key value k or not
	 * typeCode, values are needed if you want to find a specific page content into which this new data is to be inserted
	 */
	
	public void getTableRows(int[] typeCode,HashMap<Integer, Any> values, FileHandler handler, 
			ArrayList<HashMap<String,Any>> rows,ArrayList<HashMap<String,Integer>> typeCodes, ArrayList<Integer> keys,
			String[] ColNames,int k, String operator,boolean schema, boolean bruteForce,String tableName,boolean filter) throws Exception {
		
		// if bruteForce = true, traverse to the most left leaf node of B-tree and go over all leaf nodes using the rightmost pointer
		// key can be any value, note that table name is required to select from schema table, key is used to filter the result
		
		if (bruteForce || schema){
			int pagNum = traversePage(null,null,handler.getRoot(),schema,bruteForce,true,handler,tableName);
			Page p = preparePage(pagNum,handler);
			int nextLeafPage; // next leaf page pointer = 0 -> null
			do{
				nextLeafPage = getLeafPagePointer(p,handler.getRFile());
				if(p.cellNum!=0)
					readPageRecords(handler,p,rows,typeCodes,keys,ColNames,k,operator,schema,tableName,filter);
				p = preparePage(nextLeafPage,handler);
			}while(nextLeafPage!=0);
		}
		else{
			// normally search through the B-tree
			int pagNum = traversePage(typeCode,values,handler.getRoot(),schema,bruteForce,true,handler,tableName);
			Page p = preparePage(pagNum,handler);
			if(p.cellNum!=0)
				readPageRecords(handler,p,rows,typeCodes,keys,ColNames,k,operator,schema,tableName,filter);
		}
	}
	
	/*
	 * @param rfile random access file
	 * @param p the page to read
	 * @param rows row container
	 * @param typeCodes typeCode container
	 * @param keys key container
	 * @param ColNames the name list of the table
	 * @param key key value to filter tuples (only select those tuples meet the condition (k operator val))
	 * @param operator 
	 * @param schema if the table is a schema table or not
	 * @param tableName the name of the table
	 * @param filter filter tuples according to key value
	 */
	public void readPageRecords(FileHandler handler,Page p,ArrayList<HashMap<String,Any>> rows,ArrayList<HashMap<String,Integer>> typeCodes, ArrayList<Integer> keys, String[] ColNames, int key, String operator, boolean schema, String tableName, boolean filter) throws Exception{
		RandomAccessFile rfile = handler.getRFile();
		if(p.cellNum == 0)
			return;
		long baseAddress = p.pagNumber*FileHandler.pageSize;
		int[] pointers = scanForCellPointers(p,handler);
		org.omg.CORBA.ORB orb = ORB.init();
		ArrayList<Integer> typeCode = new ArrayList<>();
		
		for(int i=0;i<pointers.length;i++){
			
			typeCode.clear();
			
			long address = baseAddress + pointers[i];
			
			// read integer key
			rfile.seek(address+3);
			int rec_key = rfile.readInt();
			
			// read # of bytes in the payload header
			rfile.seek(address+7);
			int payloadHeaderSize = rfile.readShort();
			
			if(!schema){
				// filter tuples based on primary key value
				// key operator rec_key
				// filter = true if the search condition is based on primary key (it is integer) or rowId (if it is not integer) 
				// filter = false if the search condition is based on other fields
				if(filter && !checkCondition(rec_key,key,operator))
					continue;
				else{
					keys.add(rec_key);
					
					rfile.seek(address+9);
					
					HashMap<String,Integer> map2 = new HashMap<>();
					// read type codes
					for(int idx=0;idx<payloadHeaderSize-2;idx++){
						int code = rfile.readByte();
						typeCode.add(code);
						map2.put(ColNames[idx], code);
					}
					
					HashMap<String,Any> map = new HashMap<>();
					// read values
					for(int idx=0;idx<typeCode.size();idx++){
						Any any = orb.create_any();
						readValue(any,typeCode.get(idx),rfile);
						map.put(ColNames[idx], any);
					}
					rows.add(map);
					typeCodes.add(map2);
				}
			}
			else{
				// read table name 
				int length = 0;
				rfile.seek(address+9);				
				length += (rfile.readByte()-0x0C);
				length += (rfile.readByte()-0x0C);
				byte typeCode1 = rfile.readByte();
				rfile.seek(address+1+2+4+payloadHeaderSize+length);
				Any tmp = orb.create_any();
				readValue(tmp,typeCode1,rfile);
				String rec_tableName = tmp.extract_string().trim();
				
				if(filter && !((tableName.trim()).equals(rec_tableName)))
					continue;
				else{
					keys.add(rec_key);
					rfile.seek(address+9);
				
					HashMap<String,Integer> map2 = new HashMap<>();
					// read type codes
					for(int idx=0;idx<payloadHeaderSize-2;idx++){
						int code = rfile.readByte();
						typeCode.add(code);
						map2.put(ColNames[idx], code);
					}
					
					HashMap<String,Any> map = new HashMap<>();
					// read values
					for(int idx=0;idx<typeCode.size();idx++){
						Any any = orb.create_any();
						readValue(any,typeCode.get(idx),rfile);
						map.put(ColNames[idx], any);
					}	
					rows.add(map);	
					typeCodes.add(map2);
				}
			}
		}
	}
	
	private boolean checkCondition(int rec_key, int key, String operator) throws Exception {
		switch(operator){
		case "=":
			return key==rec_key;
		case ">":
			return key>rec_key;
		case "<":
			return key<rec_key;
		case "!=":
			return key!=rec_key;
		case "<>":
			return key!=rec_key;
		case ">=":
			return key>=rec_key;
		case"<=":
			return key<=rec_key;
		default:
			throw new Exception("Error 07: Unsupported comparison operation!");
		}
	}

	private void readValue(Any any, int typeCode,RandomAccessFile rfile) throws IOException {
		switch(typeCode){
		case 0x00:
			 any.insert_string("null");
			 rfile.readByte();
			 return;
		case 0x01:
			 any.insert_string("null");
			 rfile.readShort();
			 return;
		case 0x02:
			 any.insert_string("null");
			 rfile.readInt();
			 return;
		case 0x03:
			 any.insert_string("null");
			 rfile.readLong();
			 return;
		case 0x04:
			byte val = rfile.readByte();
			any.insert_octet(val);
			return;
		case 0x05:
			short val1 = rfile.readShort();
			any.insert_short(val1);
			return;
		case 0x06:
			int val2 = rfile.readInt();
			any.insert_long(val2);
			return;
		case 0x07:
			long val3 = rfile.readLong();
			any.insert_longlong(val3);
			return;
		case 0x08:
			float val4 = rfile.readFloat();
			any.insert_float(val4);
			return;
		case 0x09:
			double val5 = rfile.readDouble();
			any.insert_double(val5);
			return;
		case 0x0A:
			long val6 = rfile.readLong();
			any.insert_longlong(val6);
			return;
		case 0x0B:
			long val7 = rfile.readLong();
			any.insert_longlong(val7);
			return;
		default:
			if(typeCode>=0x0C){
				String s;
				if(typeCode>0x0C){
					byte[] charArray = new byte[typeCode-0x0C];
					rfile.readFully(charArray);
					s = new String(charArray,"UTF-8");
				}
				else
					s = "";
				any.insert_string(s);
				return;
			}
		}		
	}

	private int getLeafPagePointer(Page p,RandomAccessFile rfile) throws IOException{
		long address = (p.pagNumber)*FileHandler.pageSize+0x08;
		rfile.seek(address);
		return rfile.readInt();
	}

	// get schema table handler
	public FileHandler[] openSchema() throws IOException{
		String cwd = System.getProperty("user.dir");
		String path = cwd + "/davisbase_schemas";
		FileHandler[] handler = new FileHandler[2];
		handler[0] = new FileHandler(path+"/davisbase_tables"+"/davisbase_tables.tbl",true);
		handler[1] = new FileHandler(path+"/davisbase_columns"+"/davisbase_columns.tbl",true);
		return handler;
	}
	
	/*
	 * @param typeCodes type code array of new data
	 * @param values values of new data
	 * @param p page start split
	 * @param handler file handler
	 * @param schema  table is a schema table or not
	 * @param tableName name of the table
	 * @return root of the B+-tree
	 */
	private void split(int[] typeCodes,HashMap<Integer,Any> values,Page p, FileHandler handler, boolean schema, String tableName) throws Exception{

		if(p == null)
			return;
		if(p.cellNum+1<FileHandler.degree){
			// insert record to the page
			long baseAddress = p.pagNumber*FileHandler.pageSize;
			int lpointer = scanForLastCellPointer(p,handler);
			long address = baseAddress+lpointer;

			int[] cellPointers = scanForCellPointers(p,handler);
			
			int keyVal;
			if(p.leaf)
				keyVal = getPriMaryKeyVal(typeCodes,values,handler.getRoot(),schema,tableName,handler);	
			else
				keyVal = values.get(1).extract_long(); // values: <0,pointer>,<1,keyVal> for internal node
			byte[] record = Utilities.convertoBytes(typeCodes, values, p.leaf, keyVal);
			
			// write the record
			rfile.seek(address - record.length);
			rfile.write(record);
			
			// write pointer
			long pageHeaderEnd = scanForPageHeaderEnd(p);
			ArrayList<Integer> pointer = Utilities.toArrayList(cellPointers);
			pointer.add((int) (address-record.length-p.pagNumber*FileHandler.pageSize));
			int[] pointers = sortCellPointers(pointer,p,handler);
			
			// write cell pointers
			rfile.seek(pageHeaderEnd);
			for(int i=0;i<pointers.length;i++){
				rfile.writeShort(pointers[i]);
			}
			
			// update page information
			p.cellNum++;
			int[] tmp = scanForFBlockandFrag(p,handler);
			p.freeBlockStart = tmp[0];
			p.fragNum = tmp [1];
			writePageHeader(p);
			return;
		}
		else{
			// read page information
			int[] pointers = handler.scanForCellPointers(p,handler);
			int[] keyVals = handler.getPageKeyVals(pointers, p,handler);
			
			// get primary key value of new data
			int keyVal;
			if(p.leaf)
				keyVal = getPriMaryKeyVal(typeCodes,values,handler.getRoot(),schema,tableName,handler);
			else
				keyVal = values.get(1).extract_long(); // values: <0,pointer>,<1,keyVal> for internal node
		
		
		
			// build the key to pointer pair
			HashMap<Integer,Integer> key2pointers = Utilities.Key2Pointer(keyVals, pointers);
			ArrayList<Integer> tmp = Utilities.toArrayList(keyVals);
			tmp.add(keyVal);
			int[] newKeys = Utilities.toIntArray(tmp.toArray(new Integer[]{}));
			Arrays.sort(newKeys);
		
			// check the pivot
			int pivotIdx = (0+newKeys.length-1)/2;
			int pivot = newKeys[pivotIdx];
						
			// get pivot record
			long baseAddress = p.pagNumber*FileHandler.pageSize;
			RandomAccessFile rfile = handler.getRFile();
			byte[] pivotRecord;
			HashMap<Integer,byte[]> records = new HashMap<>();
			
			// page is leaf or not 
			// if pivot is the newly inserted value, there is no need for read pivot record, else read the record
			if(!key2pointers.containsKey(pivot)){
				int recordLength = Utilities.calRecordLength(typeCodes, p.leaf);
				pivotRecord = new byte[recordLength];
				pivotRecord = Utilities.convertoBytes(typeCodes,values,p.leaf,pivot);		
			}
			else{
				pivotRecord = readRecord(key2pointers.get(pivot),baseAddress,p.leaf,rfile);
			}
				
			// read all the records including the newly inserted one in the order of key values
			for(int i=0;i<newKeys.length;i++){
				if(newKeys[i] == pivot)
					records.put(pivot,pivotRecord);
				else{
					if(key2pointers.containsKey(newKeys[i])){
						byte[] tmp1 = readRecord(key2pointers.get(newKeys[i]),baseAddress,p.leaf,rfile);
						records.put(newKeys[i],tmp1);
					}
					else{
						byte[] tmp1 = Utilities.convertoBytes(typeCodes, values, p.leaf,keyVal);
						records.put(newKeys[i],tmp1);
					}
				}			
			}
			
			
			// if this page has a parent page, split into two pages and insert the pointer into the parent page
			if(p.parentPage != null){
				// if p is leaf, copy all records >= pivot to new page, other records left in original page, set original page righmost pointer points to new page
				// insert cellPointer to the parent node
				// if p is not leaf, everything is same except pivot record is ignored
				// if p is an internal node, remember to set the left child page header right pointer = pivot left pointer before spliting. 
				if(p.leaf){
					Page newP = new Page(this.databasePages);
					newP.parentPage = p.parentPage;
					newP.leaf=true;
					newP.schemaRoot=false;

					// create the pointer cell inserted into a internal node
					int[] newTypeCodes = new int[]{0x06,0x06};
					HashMap<Integer,Any> newValues= new HashMap<>();
					org.omg.CORBA.ORB orb = org.omg.CORBA.ORB.init();
					Any any = orb.create_any();
					Any any1 = orb.create_any();
					any.insert_long(newP.pagNumber);
					any1.insert_long(pivot);
					newValues.put(0, any);
					newValues.put(1, any1);
					
					// update original page right pointer and newP right pointer
					newP.rightPointer = p.pagNumber;
					
					
					// clear the page
					for(int i=0;i<pointers.length;i++){
						rfile.seek(baseAddress+pointers[i]);
						rfile.writeByte(1);
					}
					
				    long pos = (p.pagNumber+1)*FileHandler.pageSize-this.uspace; // move to the end of page p 
				    long pos2 = (newP.pagNumber+1)*FileHandler.pageSize-this.uspace; // move to the end of page newP
				    newP.cellStart = (int) (pos2 - newP.pagNumber*FileHandler.pageSize)-1;
				    p.cellStart = (int) (pos - p.pagNumber*FileHandler.pageSize)-1;
				    
				    ArrayList<Integer> cellPointer_p = new ArrayList<>();
				    ArrayList<Integer> cellPointer_newP = new ArrayList<>();
				    int countP = 0;
					
				    // write records on page p and page newP
					for(int i=0;i<newKeys.length;i++){
						if (newKeys[i]<pivot){
							byte[] record = records.get(newKeys[i]);
							pos2 = pos2-record.length; // move to the start position to write the record
							rfile.seek(pos2);
							rfile.write(record);
							cellPointer_newP.add((int) (pos2-(newP.pagNumber*FileHandler.pageSize)));
							countP++;
						}
						else{
							byte[] record = records.get(newKeys[i]);
							pos = pos-record.length;
							rfile.seek(pos);
							rfile.write(record);
							cellPointer_p.add((int) (pos-(p.pagNumber*FileHandler.pageSize)));
						}
					}
					// sort cell pointers
					int[] cellPointers_p = sortCellPointers(cellPointer_p,p,handler);
					int[] cellPointers_newP = sortCellPointers(cellPointer_newP,newP,handler);
					
					// update page information
					newP.cellNum = countP;
					p.cellNum = newKeys.length-countP;
					
					// write cell pointers
					long pageEnd_p = scanForPageHeaderEnd(p);
					long pageEnd_newP = scanForPageHeaderEnd(newP);
					
					rfile.seek(pageEnd_p);
					for(int i=0;i<cellPointers_p.length;i++)
						rfile.writeShort(cellPointers_p[i]);
					
					rfile.seek(pageEnd_newP);
					for(int i=0;i<cellPointers_newP.length;i++)
						rfile.writeShort(cellPointers_newP[i]);
					
					int[] tmp1 = scanForFBlockandFrag(p,handler);
					p.freeBlockStart = tmp1[0];
					p.fragNum = tmp1[1];
					this.fileChangeCounter++;
					writePageHeader(p);
					
					
					tmp1 = scanForFBlockandFrag(newP,handler);
					newP.freeBlockStart = tmp1[0];
					newP.fragNum = tmp1[1];
					this.fileChangeCounter++;
					writePageHeader(newP);
					
					// update the previous leaf page right pointer
					int[] tmp_pointers = scanForCellPointers(p.parentPage,handler);
					long tmp_baseAddress = p.parentPage.pagNumber*FileHandler.pageSize;
					long target_address = -1;
					int idx = -2;
					for(int i=0;i<tmp_pointers.length;i++){
						long tmp_address = tmp_baseAddress+tmp_pointers[i];
						rfile.seek(tmp_address+1);
						int tmp_pointer = rfile.readInt();
						if(tmp_pointer == p.pagNumber){
							if(i == 0)
								idx = i;
							else{
								idx = i-1;
								target_address = tmp_baseAddress + tmp_pointers[i-1]+1;
							}
							break;
						}
					}
					if(idx == -2){
						target_address = tmp_baseAddress+tmp_pointers[tmp_pointers.length-1]+1;
					}
					if(idx!=0){
						rfile.seek(target_address);
						int page = rfile.readInt();
						rfile.seek(page*FileHandler.pageSize+0x08);
						rfile.writeInt(newP.pagNumber);
					}
					
					
					split(newTypeCodes,newValues,p.parentPage,handler,schema,tableName);
				}
				
				else{
					// p is an internal node
					// original page is the right page, new page is the left one
					// change the pivot left pointer to the header right pointer of new page and update the pivot with left pointer that points to new page to the parent page
					
					Page newP = new Page(this.databasePages);
					newP.parentPage = p.parentPage;
					newP.leaf=false;
					newP.schemaRoot=false;
					
					// create the pointer cell inserted into a internal node
					int[] newTypeCodes = new int[]{0x06,0x06};
					HashMap<Integer,Any> newValues= new HashMap<>();
					org.omg.CORBA.ORB orb = org.omg.CORBA.ORB.init();
					Any any = orb.create_any();
					Any any1 = orb.create_any();
					any.insert_long(newP.pagNumber);
					any1.insert_long(pivot);
					newValues.put(0, any);
					newValues.put(1, any1);
					
					// find the pivot left pointer value
					int pivot_leftPointer;
					if(key2pointers.containsKey(pivot)){
						long tmp_address = p.pagNumber*FileHandler.pageSize + key2pointers.get(pivot)+1;
						rfile.seek(tmp_address);
						pivot_leftPointer = rfile.readInt();
					}
					else{
						pivot_leftPointer = values.get(0).extract_long();
					}
					
					// clear the page
					for(int i=0;i<pointers.length;i++){
						rfile.seek(baseAddress+pointers[i]);
						rfile.writeByte(1);
					}
					
				    long pos = (p.pagNumber+1)*FileHandler.pageSize-this.uspace; 	 // move to the end of page p 
				    long pos2 = (newP.pagNumber+1)*FileHandler.pageSize-this.uspace; // move to the end of page newP
				    newP.cellStart = (int) (pos2 - newP.pagNumber*FileHandler.pageSize)-1;
				    p.cellStart = (int) (pos - p.pagNumber*FileHandler.pageSize)-1;
				    
				    ArrayList<Integer> cellPointer_p = new ArrayList<>();
				    ArrayList<Integer> cellPointer_newP = new ArrayList<>();
				    int countP = 0;
					
				    // write records on page p and page newP
				    // records with key<pivot go to new page, others remain in page p
					for(int i=0;i<newKeys.length;i++){
						if (newKeys[i]<pivot){
							byte[] record = records.get(newKeys[i]);
							pos2 = pos2-record.length; // move to the start position to write the record
							rfile.seek(pos2);
							rfile.write(record);
							cellPointer_newP.add((int) (pos2-(newP.pagNumber*FileHandler.pageSize)));
							countP++;
						}
						else if(newKeys[i]>pivot){
							byte[] record = records.get(newKeys[i]);
							pos = pos-record.length;
							rfile.seek(pos);
							rfile.write(record);
							cellPointer_p.add((int) (pos-(p.pagNumber*FileHandler.pageSize)));
						}
					}
					// sort cell pointers
					int[] cellPointers_p = sortCellPointers(cellPointer_p,p,handler);
					int[] cellPointers_newP = sortCellPointers(cellPointer_newP,newP,handler);
					
					// update page information
					newP.cellNum = countP;
					p.cellNum = newKeys.length-1-countP;
					
					// write cell pointers
					long pageEnd_p = scanForPageHeaderEnd(p);
					long pageEnd_newP = scanForPageHeaderEnd(newP);
					
					rfile.seek(pageEnd_p);
					for(int i=0;i<cellPointers_p.length;i++)
						rfile.writeShort(cellPointers_p[i]);
					
					rfile.seek(pageEnd_newP);
					for(int i=0;i<cellPointers_newP.length;i++)
						rfile.writeShort(cellPointers_newP[i]);
					
					int[] tmp1 = scanForFBlockandFrag(p,handler);
					p.freeBlockStart = tmp1[0];
					p.fragNum = tmp1[1];
					this.fileChangeCounter++;
					writePageHeader(p);
					
					
					tmp1 = scanForFBlockandFrag(newP,handler);
					newP.freeBlockStart = tmp1[0];
					newP.fragNum = tmp1[1];
					// copy pivot left pointer to the left node right pointer
					newP.rightPointer = pivot_leftPointer;
					this.fileChangeCounter++;
					writePageHeader(newP);
					
					split(newTypeCodes,newValues,p.parentPage,handler,schema,tableName);
				}
			}
			// there is no parent page: either a single leaf page split or a single internal node split
			// since in davisbase, root is assumed to be page 1.
			else{
				if(p.leaf){
					
					Page newP2 = new Page(this.databasePages);
					newP2.parentPage = p;
					newP2.leaf=true;
					newP2.schemaRoot=false;
					
					Page newP = new Page(this.databasePages);
					newP.parentPage = p;
					newP.leaf=true;
					newP.schemaRoot=false;
					
					// update original page right pointer and newP right pointer
					newP.rightPointer = newP2.pagNumber;
					newP2.rightPointer = 0;
					p.rightPointer = newP2.pagNumber;
					
					// clear the page
					for(int i=0;i<pointers.length;i++){
						rfile.seek(baseAddress+pointers[i]);
						rfile.writeByte(1);
					}
					
				    long pos = (p.pagNumber+1)*FileHandler.pageSize-this.uspace;     // move to the end of page p 
				    long pos2 = (newP.pagNumber+1)*FileHandler.pageSize-this.uspace; // move to the end of page newP
				    long pos3 = (newP2.pagNumber+1)*FileHandler.pageSize-this.uspace; // move to the end of page newP2
				    newP.cellStart = (int) (pos2 - newP.pagNumber*FileHandler.pageSize)-1;
				    newP2.cellStart = (int) (pos3 - newP2.pagNumber*FileHandler.pageSize)-1;
				    p.cellStart = (int) (pos - p.pagNumber*FileHandler.pageSize)-1;
				    
				    
				    ArrayList<Integer> cellPointer_newP = new ArrayList<>();
				    ArrayList<Integer> cellPointer_newP2 = new ArrayList<>();
				    int countP = 0;
					
				    // write records on page newP and page newP2
					for(int i=0;i<newKeys.length;i++){
						if (newKeys[i]<pivot){
							byte[] record = records.get(newKeys[i]);
							pos2 = pos2-record.length; // move to the start position to write the record
							rfile.seek(pos2);
							rfile.write(record);
							cellPointer_newP.add((int) (pos2-(newP.pagNumber*FileHandler.pageSize)));
							countP++;
						}
						else{
							byte[] record = records.get(newKeys[i]);
							pos3 = pos3-record.length;
							rfile.seek(pos3);
							rfile.write(record);
							cellPointer_newP2.add((int) (pos3-(newP2.pagNumber*FileHandler.pageSize)));
						}
					}
					// sort cell pointers
					int[] cellPointers_newP = sortCellPointers(cellPointer_newP,newP,handler);
					int[] cellPointers_newP2 = sortCellPointers(cellPointer_newP2,newP2,handler);
					
					// update page information
					newP.cellNum = countP;
					newP2.cellNum = newKeys.length-countP;
					
					// write cell pointers
					long pageEnd_newP = scanForPageHeaderEnd(newP);
					long pageEnd_newP2 = scanForPageHeaderEnd(newP2);
					
					rfile.seek(pageEnd_newP);
					for(int i=0;i<cellPointers_newP.length;i++)
						rfile.writeShort(cellPointers_newP[i]);
					
					rfile.seek(pageEnd_newP2);
					for(int i=0;i<cellPointers_newP2.length;i++)
						rfile.writeShort(cellPointers_newP2[i]);
					
					int[] tmp1 = scanForFBlockandFrag(newP,handler);
					newP.freeBlockStart = tmp1[0];
					newP.fragNum = tmp1[1];
					this.fileChangeCounter++;
					writePageHeader(newP);
					
					
					tmp1 = scanForFBlockandFrag(newP2,handler);
					newP2.freeBlockStart = tmp1[0];
					newP2.fragNum = tmp1[1];
					this.fileChangeCounter++;
					writePageHeader(newP2);
					
					// build internal cell 
					HashMap<Integer,Any> tmp_any = new HashMap<>();
					ORB orb = org.omg.CORBA.ORB.init();
					Any tmp_any2 = orb .create_any();
					tmp_any2.insert_long(newP.pagNumber);
					tmp_any.put(0, tmp_any2);
					byte[] internal_record = Utilities.convertoBytes(null, tmp_any, false, pivot);
					rfile.seek(pos-internal_record.length);
					rfile.write(internal_record);
					
					p.cellNum = 1;
					p.fragNum = 0;
					p.freeBlockStart = 0;
					p.leaf = false;
					p.parentPage = null;
					p.schemaRoot = false;
					writePageHeader(p);
					// write internal node cell pointer
					long pageEnd_p = scanForPageHeaderEnd(p);
					rfile.seek(pageEnd_p);
					rfile.writeShort((int) (pos-internal_record.length-(p.pagNumber*FileHandler.pageSize)));
				}
				else{
					// p is an internal node
					// original page is the right page, new page is the left one
					// change the pivot left pointer to the header right pointer of new page and update the pivot with left pointer that points to new page to the parent page
					
					Page newP = new Page(this.databasePages);
					newP.parentPage = p;
					newP.leaf=false;
					newP.schemaRoot=false;
					
					Page newP2 = new Page(this.databasePages);
					newP2.parentPage = p;
					newP2.leaf=false;
					newP2.schemaRoot=false;
					
					
					// find the pivot left pointer value
					int pivot_leftPointer;
					if(key2pointers.containsKey(pivot)){
						long tmp_address = p.pagNumber*FileHandler.pageSize + key2pointers.get(pivot)+1;
						rfile.seek(tmp_address);
						pivot_leftPointer = rfile.readInt();
					}
					else{
						pivot_leftPointer = values.get(0).extract_long();
					}
					
					// copy right pointer
					newP2.rightPointer = p.rightPointer;
					p.rightPointer = newP2.pagNumber;
					
					// clear the page p
					for(int i=0;i<pointers.length;i++){
						rfile.seek(baseAddress+pointers[i]);
						rfile.writeByte(1);
					}
					
				    long pos = (p.pagNumber+1)*FileHandler.pageSize-this.uspace; 	 // move to the end of page p 
				    long pos2 = (newP.pagNumber+1)*FileHandler.pageSize-this.uspace; // move to the end of page newP
				    long pos3 = (newP2.pagNumber+1)*FileHandler.pageSize-this.uspace; // move to the end of page newP2
				    newP.cellStart = (int) (pos2 - newP.pagNumber*FileHandler.pageSize)-1;
				    newP2.cellStart = (int) (pos3 - newP2.pagNumber*FileHandler.pageSize)-1;
				    p.cellStart = (int) (pos - p.pagNumber*FileHandler.pageSize)-1;
				    
				    ArrayList<Integer> cellPointer_newP = new ArrayList<>();
				    ArrayList<Integer> cellPointer_newP2 = new ArrayList<>();
				    int countP = 0;
					
				    // write records on page newP and page newP2
				    // records with key<pivot go to newP, key>pivot go to newP2
					for(int i=0;i<newKeys.length;i++){
						if (newKeys[i]<pivot){
							byte[] record = records.get(newKeys[i]);
							pos2 = pos2-record.length; // move to the start position to write the record
							rfile.seek(pos2);
							rfile.write(record);
							cellPointer_newP.add((int) (pos2-(newP.pagNumber*FileHandler.pageSize)));
							countP++;
						}
						else if(newKeys[i]>pivot){
							byte[] record = records.get(newKeys[i]);
							pos3 = pos3-record.length;
							rfile.seek(pos3);
							rfile.write(record);
							cellPointer_newP2.add((int) (pos3-(newP2.pagNumber*FileHandler.pageSize)));
						}
					}
					// sort cell pointers
					int[] cellPointers_newP = sortCellPointers(cellPointer_newP,newP,handler);
					int[] cellPointers_newP2 = sortCellPointers(cellPointer_newP2,newP2,handler);
					
					// update page information
					newP.cellNum = countP;
					newP2.cellNum = newKeys.length-1-countP;
					
					// write cell pointers
					long pageEnd_newP = scanForPageHeaderEnd(newP);
					long pageEnd_newP2 = scanForPageHeaderEnd(newP2);
					
					rfile.seek(pageEnd_newP);
					for(int i=0;i<cellPointers_newP.length;i++)
						rfile.writeShort(cellPointers_newP[i]);
					
					rfile.seek(pageEnd_newP2);
					for(int i=0;i<cellPointers_newP2.length;i++)
						rfile.writeShort(cellPointers_newP2[i]);
					
					int[] tmp1 = scanForFBlockandFrag(newP2,handler);
					newP2.freeBlockStart = tmp1[0];
					newP2.fragNum = tmp1[1];
					this.fileChangeCounter++;
					writePageHeader(newP2);
					
					
					tmp1 = scanForFBlockandFrag(newP,handler);
					newP.freeBlockStart = tmp1[0];
					newP.fragNum = tmp1[1];
					// copy pivot left pointer to the left node right pointer
					newP.rightPointer = pivot_leftPointer;
					this.fileChangeCounter++;
					writePageHeader(newP);
					
					
					// build internal cell 
					HashMap<Integer,Any> tmp_any = new HashMap<>();
					ORB orb = org.omg.CORBA.ORB.init();
					Any tmp_any2 = orb .create_any();
					tmp_any2.insert_long(newP.pagNumber);
					tmp_any.put(0, tmp_any2);
					byte[] internal_record = Utilities.convertoBytes(null, tmp_any, false, pivot);
					rfile.seek(pos-internal_record.length);
					rfile.write(internal_record);
					
					p.cellNum = 1;
					p.fragNum = 0;
					p.freeBlockStart = 0;
					p.leaf = false;
					p.parentPage = null;
					p.schemaRoot = false;
					writePageHeader(p);
					long pageEnd_p = scanForPageHeaderEnd(p);
					rfile.seek(pageEnd_p);
					rfile.writeShort((int) (pos-internal_record.length-(p.pagNumber*FileHandler.pageSize)));
				}
			}
		}
	}
	
	
	
	/*
	 * scan for the pointer and return its binary file address
	 * @param pointer page pointer
	 * @param p page to scan
	 * @return binary address of the pointer
	 */
	
	public long scanForPointer(int pointer, Page p,FileHandler handler) throws Exception{
		RandomAccessFile rfile = handler.getRFile();
		long baseAddress = p.pagNumber*FileHandler.pageSize;
		int[] cellPointers = scanForCellPointers(p,handler);
		for(int i=0;i<cellPointers.length;i++){
			long address = baseAddress + cellPointers[i] +1;
			rfile.seek(address);
			int tmp_pointer = rfile.readInt();
			if(tmp_pointer == pointer)
				return address;
		}
		rfile.seek(baseAddress+0x08);
		int tmp_pointer = rfile.readInt();
		if(tmp_pointer == pointer)
			return baseAddress+0x08;
		else
			throw new Exception("Error 09: can't find the pointer in the parent page");
		
	}
	
	/*
	 * scan for a specific key value and return its associated value 
	 */
	
	/*
	 *  read record and return a byte array
	 *  @param pointer cell pointer of the cell record
	 *  @param baseAddress base address of the page
	 *  @param leaf page is leaf or not
	 *  @param rfile random access file
	 *  @return byte array of the record
	 */
	private byte[] readRecord(int pointer, long baseAddress,boolean leaf,RandomAccessFile rfile) throws IOException{
		byte[] record;
		if(leaf){
			rfile.seek(baseAddress+pointer+1);
			int payloadLength = rfile.readShort();
			record = new byte[1+2+4+payloadLength];
			rfile.seek(baseAddress+pointer);

			rfile.readFully(record);
		}
		else{
			record = new byte[8+1];
			rfile.seek(baseAddress+pointer);
			rfile.readFully(record);
		}
		return record;
	}
	
	/*
	 * read table information in the order of insertion.
	 */
	public void prepareTableSchemaInfo(String tableName) throws Exception{
		ArrayList<HashMap<String,Any>> rows = new ArrayList<>();
		ArrayList<HashMap<String,Integer>> typeCodes = new ArrayList<>();
		ArrayList<Integer> keys = new ArrayList<>();
		FileHandler[] handler = openSchema();
		getTableRows(null,null,handler[1],rows,typeCodes,keys,FileHandler.davisbase_column_header,-1,null,true,true,tableName,true);
		handler[0].close();
		handler[1].close();
		this.rows = rows;
		this.typeCodes = typeCodes;
		this.keys = keys;
	}
	
	public String[] getTableColNames(String tableName) throws Exception{
		prepareTableSchemaInfo(tableName);
		String[] names = new String[this.rows.size()];
		for(int i=0;i<this.rows.size();i++){
			HashMap<String,Any> map = this.rows.get(i);
			names[i] = map.get("COLUMN_NAME").extract_string().trim();
		}
		return names;
	}
	
	public String[] getTableColNames() throws Exception{
		String[] names = new String[this.rows.size()];
		for(int i=0;i<this.rows.size();i++){
			HashMap<String,Any> map = this.rows.get(i);
			names[i] = map.get("COLUMN_NAME").extract_string().trim();
		}
		return names;
	}
	
	public HashMap<String,HashMap<String,String>> getTableConstraints(String tableName) throws Exception{
		prepareTableSchemaInfo(tableName);
		
		
		String[] names = new String[this.rows.size()];
		HashMap<String,String> constraints;
		HashMap<String,HashMap<String,String>> result = new HashMap<>();
		for(int i=0;i<this.rows.size();i++){
			HashMap<String,Any> map = this.rows.get(i);
			names[i] = map.get("COLUMN_NAME").extract_string().trim();
			String is_nullable = map.get("IS_NULLABLE").extract_string().trim();
			String data_type = map.get("DATA_TYPE").extract_string().trim();
			String char_max_length = map.get("CHAR_MAX_LENGTH").extract_string().trim();
			String column_key = map.get("COLUMN_KEY").extract_string().trim();
			String column_default = map.get("COLUMN_DEFAULT").extract_string().trim();
			constraints = new HashMap<>();
			constraints.put("IS_NULLABLE", is_nullable);
			constraints.put("DATA_TYPE",data_type);
			constraints.put("CHAR_MAX_LENGTH", char_max_length);
			constraints.put("COLUMN_KEY", column_key);
			constraints.put("COLUMN_DEFAULT", column_default);
			result.put(names[i], constraints);
		}
		return result;
	}
	
	
	/*
	 *  check if the key column of a table is integer or not
	 */
	public boolean checkPriInt(String tableName) throws Exception{
		// refer to schema table and get the corresponding column list in the order when they are inserted.
		// read columns table and get all corresponding rows for the table
		ArrayList<HashMap<String, Any>> rows = new ArrayList<>();
		ArrayList<HashMap<String,Integer>> type = new ArrayList<>();
		ArrayList<Integer> keys = new ArrayList<>();
		FileHandler[] handler = openSchema();
		
		getTableRows(null,null,handler[1],rows,type,keys,FileHandler.davisbase_column_header,-1,"=",true,true,tableName, true);
					
		handler[0].close();
		handler[1].close();
					
		// check which column is the primary key field
		int idx = checkPriColumn(rows);
		boolean priInt = false;
		if(idx != -1){
				HashMap<String,Any> priCol = rows.get(idx);
				priInt = checkPriInt(priCol);
		}
		return priInt;
	}
	
	
	
	/*
	 * write the record into the page
	 * @param type code array of new data
	 * @param values values of new data
	 * @param pointerStart start position to write pointer array
	 * @param writeLocation start position to write cell
	 * @param p page into which the new data is written
	 * @param recordLength length of the new record
	 * @param schema the table is a schema table or not
	 * @param leftPointer leftPointer of cell of an internal node
	 * @param cellPointers cell pointers in the page
	 * @param tableName name of the table 
	 */
	private void writeRecord(FileHandler handler, int[] typeCodes, HashMap<Integer,Any> values,long pointerStart, long writeLocation, Page p, int recordLength, boolean schema,int leftPointer,int[] cellPointers,String tableName) throws Exception{
		RandomAccessFile rfile = handler.getRFile();
		long baseAddress = p.pagNumber*FileHandler.pageSize;

		// note that for insertion, it requires to insert the cell first
		// write cell
		// move to cell start
		rfile.seek(writeLocation);
		
		
		// write deleteMarker
		rfile.write(0);
		
		if(p.leaf){
			rfile.seek(writeLocation+1);
			rfile.writeShort(recordLength-1-2-4);


			int priKeyVal = getPriMaryKeyVal(typeCodes,values,p,schema,tableName,handler);
			rfile.seek(writeLocation+3);
			rfile.writeInt(priKeyVal);
			

			rfile.writeShort(typeCodes.length+2);


			
			for(int i=0;i<typeCodes.length;i++)
				rfile.writeByte(typeCodes[i]);
			for(int i=0;i<typeCodes.length;i++){
				int typeCode = typeCodes[i];
				writeValue(i,typeCode,values,handler);
			}
		}
		else{
			rfile.writeInt(leftPointer);
			int priKeyVal = getPriMaryKeyVal(typeCodes,values,p,schema,tableName,handler);
			rfile.seek(writeLocation+5);
			rfile.writeInt(priKeyVal);
		}
		
		// write cell pointer
		rfile.seek(pointerStart);
		if(cellPointers == null)
			rfile.writeShort((int) (writeLocation-baseAddress));
		else{
			ArrayList<Integer> pointers = Utilities.toArrayList(cellPointers);
			pointers.add((int)(writeLocation-baseAddress));
					
			// get ordered pointer array
			cellPointers = sortCellPointers(pointers,p,handler);
			
			// write pointers
			// move to the start position of cell pointers
			long pos = pointerStart;
			for(int i=0;i<cellPointers.length;i++){
				rfile.seek(pos);
				rfile.writeShort(cellPointers[i]);
				pos+=2;
			}
		}
	}
	
	/*
	 *  sort cell pointers
	 */
	private int[] sortCellPointers(ArrayList<Integer> pointers, Page p, FileHandler handler) throws IOException {
		int[] array = Utilities.toIntArray(pointers.toArray(new Integer[]{}));
		int[] keyVals = getPageKeyVals(array,p,handler);
		MultiMap<Integer,Integer> key2address = new MultiMap<>();
		
		for(int i=0;i<keyVals.length;i++){
			key2address.putM(keyVals[i], array[i]);
		}
		keyVals = Utilities.removeDupKeys(keyVals);
		Arrays.sort(keyVals);
		int idx = 0;
		for(int i=0;i<keyVals.length;i++){
			Integer[] tmp2 = key2address.getM(keyVals[i]);
			for (int j=0;j<tmp2.length;j++)
				array[idx++] = tmp2[j];
		}
		return array;
	}
	
	
	
	public int[] getPageKeyVals(int[] pointers,Page p,FileHandler handler) throws IOException{
		RandomAccessFile rfile = handler.getRFile();
		int[] keys = new int[pointers.length];
		long baseAddress = p.pagNumber*FileHandler.pageSize;
		if(p.leaf){
			for(int i=0;i<pointers.length;i++){
				long address = baseAddress+pointers[i]+3;
				rfile.seek(address);
				keys[i] = rfile.readInt();
			}
		}
		else{
			for(int i = 0;i<pointers.length;i++){
				long address = baseAddress+pointers[i]+5;
				rfile.seek(address);
				keys[i] = rfile.readInt();
			}
		}
		return keys;
	}

	private void writeValue(int i, int typeCode, HashMap<Integer, Any> values, FileHandler handler) throws IOException  {
		RandomAccessFile rfile = handler.getRFile();
		Any any = values.get(i);
		switch(typeCode){
		// do not need to write null values
		case 0x00:
			 byte val8 = any.extract_octet();
			 rfile.write(val8);
			 return;
		case 0x01:
			 short val9 = any.extract_short();
			 rfile.writeShort(val9);
			 return;
		case 0x02:
			 int val10 = any.extract_long();
			 rfile.writeInt(val10);
			 return;
		case 0x03:
			 long val11 = any.extract_longlong();
			 rfile.writeLong(val11);
			 return;
		case 0x04:
			byte val = any.extract_octet();
			rfile.write(val);
			return;
		case 0x05:
			short val1 = any.extract_short();
			rfile.writeShort(val1);
			return;
		case 0x06:
			int val2 = any.extract_long();
			rfile.writeInt(val2);
			return;
		case 0x07:
			long val3 = any.extract_longlong();
			rfile.writeLong(val3);
			return;
		case 0x08:
			float val4 = any.extract_float();
			rfile.writeFloat(val4);
			return;
		case 0x09:
			double val5 = any.extract_double();
			rfile.writeDouble(val5);
			return;
		case 0x0A:
			long val6 = any.extract_longlong();
			rfile.writeLong(val6);
			return;
		case 0x0B:
			long val7 = any.extract_longlong();
			rfile.writeLong(val7);
			return;
		default:
			if(typeCode>=0x0C){
				String s = any.extract_string();
				
				if(s!=null){
					s = Utilities.buildStrWithSize(s,typeCode-0x0C-s.length());
					rfile.writeBytes(s);
				}
				else{
					s = Utilities.buildStrWithSize(s, typeCode-0x0C);
					rfile.writeBytes(s);
				}
				return;
			}
		}
	}

	private void writeValue( int typeCode, Any any, RandomAccessFile rfile) throws IOException  {
		switch(typeCode){
		// do not need to write null values
		case 0x00:
			 return;
		case 0x01:
			 return;
		case 0x02:
			return;
		case 0x03:
			return;
		case 0x04:
			byte val = any.extract_octet();
			rfile.write(val);
			return;
		case 0x05:
			short val1 = any.extract_short();
			rfile.writeShort(val1);
			return;
		case 0x06:
			int val2 = any.extract_long();
			rfile.writeInt(val2);
			return;
		case 0x07:
			long val3 = any.extract_longlong();
			rfile.writeLong(val3);
			return;
		case 0x08:
			float val4 = any.extract_float();
			rfile.writeFloat(val4);
			return;
		case 0x09:
			double val5 = any.extract_double();
			rfile.writeDouble(val5);
			return;
		case 0x0A:
			long val6 = any.extract_longlong();
			rfile.writeLong(val6);
			return;
		case 0x0B:
			long val7 = any.extract_longlong();
			rfile.writeLong(val7);
			return;
		default:
			if(typeCode>=0x0C){
				String s = any.extract_string();
				
				if(s!=null){
					s = Utilities.buildStrWithSize(s,typeCode-0x0C-s.length());
					rfile.writeBytes(s);
				}
				else{
					s = Utilities.buildStrWithSize(s, typeCode-0x0C);
					rfile.writeBytes(s);
				}
				return;
			}
		}
	}
	
	/*
	 *  get root page according to page number
	 */
	public Page getRoot() throws IOException{
		if(this.databasePages == 1)
			return null;
		// read flag
		this.rfile.seek(FileHandler.pageSize);
		byte flag = this.rfile.readByte();
		
		// read start of first free block
		this.rfile.seek(FileHandler.pageSize+0x01);
		int freeBlockStart = this.rfile.readShort();
		
		// read number of cells on the page
		this.rfile.seek(FileHandler.pageSize+0x03);
		int cellNum = this.rfile.readShort();
		
		// read start of cell content area
		this.rfile.seek(FileHandler.pageSize+0x05);
		int cellStart = this.rfile.readShort();
		
		// read number of fragment free bytes
		this.rfile.seek(FileHandler.pageSize+0x07);
		int fragNum = this.rfile.readByte();
		
		// read right pointer
		boolean leaf;
		int rightPointer;
		if (flag == 0x05){
			this.rfile.seek(FileHandler.pageSize+0x08);
			rightPointer = this.rfile.readInt();
			leaf =false;
		}
		else{
			this.rfile.seek(FileHandler.pageSize+0x08);
			leaf = true;
			rightPointer = this.rfile.readInt(); // 0 means invalid pointer
		}
		
		Page p = new Page(cellNum,cellStart,fragNum,freeBlockStart,leaf,1,null,rightPointer,false);
		return p;
	}
	
	
	public Page preparePage(int pageNum,FileHandler handler) throws IOException{
		long baseAddress = pageNum*FileHandler.pageSize;
		RandomAccessFile rfile = handler.getRFile();
		
		// read page header
		
		// read flag
		rfile.seek(baseAddress);
		byte flag = rfile.readByte();
		
		// read firstFreeBlockStart
		rfile.seek(baseAddress+0x0001);
		short freeblockStart = rfile.readShort();
		
		// read number of cells
		rfile.seek(baseAddress+0x0003);
		short cellNum = rfile.readShort();
		
		// read cell content area start region
		rfile.seek(baseAddress+0x0005);
		short cellStart = rfile.readShort();
		
		// read fragment free bytes
		rfile.seek(baseAddress+0x0007);
		byte fragFreeBytes = rfile.readByte();
		
		// read page right pointer
		rfile.seek(baseAddress+0x0008);
		int rightPointer = rfile.readInt();
		
		boolean leaf = false;
		if(flag == 0x0d)
			leaf = true;
		Page p = new Page(cellNum,cellStart,fragFreeBytes,freeblockStart,leaf,pageNum,null,rightPointer,false);
		return p;
	}
	public void prepareDatabaseHeader() throws IOException{
		// getPageSize
		getPageSize();
		
		// getFileSize
		getFileSize();
		
		// get reserved space
		getReservedSpace();
		
		// get fileChangeCounter
		this.rfile.seek(0x001C);
		int firstFileChangeCounter = this.rfile.readInt();
		
		this.rfile.seek(0x0040);
		int secondFileChangeCounter = this.rfile.readInt();
		
		if(firstFileChangeCounter == secondFileChangeCounter)
			this.fileChangeCounter = firstFileChangeCounter;
		else
			this.fileChangeCounter = Math.max(firstFileChangeCounter, secondFileChangeCounter);
	}
	
	/*
	 * get all table names, read from davisbase_tables.tbl
	 */
	
	public String[] getTableNames( FileHandler handler, String database) throws Exception {
		
			int pagNum = traversePage(null,null,handler.getRoot(),true,true,true,handler,null);
			ArrayList<String> tableNames = new ArrayList<>();
			org.omg.CORBA.ORB orb = ORB.init();
			Page p = preparePage(pagNum,handler);
			int nextLeafPage; // next leaf page pointer = 0 -> null
			do{
				nextLeafPage = getLeafPagePointer(p,handler.getRFile());
				if(p.cellNum!=0){
					long baseAddress = p.pagNumber*FileHandler.pageSize;
					int[] pointers = scanForCellPointers(p,handler);
					
					for(int i=0;i<pointers.length;i++){
						
						int length = 0;
						int length2 = 0;
						long address = baseAddress+pointers[i];

						rfile.seek(address+7);
						int payloadHeaderSize = rfile.readShort();
						rfile.seek(address+9);			
						length += (rfile.readByte()-0x0C);
						length2 = length;
						byte typeCode2 = rfile.readByte();
						length += (typeCode2-0x0C);
						byte typeCode1 = rfile.readByte();
						rfile.seek(address+1+2+4+payloadHeaderSize+length);
						Any tmp = orb.create_any();
						readValue(tmp,typeCode1,rfile);
						String rec_tableName = tmp.extract_string().trim();
						rfile.seek(address+1+2+4+payloadHeaderSize+length2);
						tmp = orb.create_any();
						readValue(tmp,typeCode2,rfile);
						String rec_schemaName = tmp.extract_string().trim();
						if(rec_schemaName.equals(database) && !tableNames.contains(rec_tableName))
							tableNames.add(rec_tableName);
					}
				}
				p = preparePage(nextLeafPage,handler);
			}while(nextLeafPage!=0);
			return tableNames.toArray(new String[]{});
	}
}
