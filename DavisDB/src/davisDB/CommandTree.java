package davisDB;
import java.util.Arrays;
import java.util.regex.*;
/*
 *  This class is used to parse the command and create a specific tree for the command
 *  command_type_code
 *         |
 *     opera_target ----------------------------------------------------> where: attr1 -> attr2 ->  attr3
 *         |																  	operator  operator  operator
 *   	attr1 -> attr2 -> attr3 -> .... -> <list of attributes>					  val1     val2     val3
 *     	   |	  |		   |				
 *   	 val1	 val2     val3 	   ....
 *   
 *   e.g. UPDATE t1 SET c1=0,c2=1,c3=2 WHERE c1 = 5;
 *   		7
 *   		|
 *   	   t1 ---------------------> c1
 *   		|	 					 =
 *   	   c1 -> c2 -> c3 			 5
 *   	    |    |     |
 *          0    1     2
 */

public class CommandTree{
	private Node root = null;
	
	public enum CommandType
	{
		SHOW_TABLES(0),CREATE_TABLE(1),CREATE_INDEX(2),DROP_TABLE(3),DROP_INDEX(4),INSERT_INTO_TABLE(5),DELETE_FROM(6),UPDATE(7),SELECT(8),CREATE_DATABASE(9),USE(10),SHOW_SCHEMAS(11);
		private int value;
		
		private CommandType(int value)
		{
			this.value = value;
		}
		
		public int getCode()
		{
			return this.value;
		}
	}
	
	class Node{
		private String Name = null;
		private int typeCode = -1;
		private byte[] value = null;
		private String[] constraints = null;
		private boolean leaf = false;
		private Node leftChild = null;
		private Node sibling = null;

		public Node(String name,byte[] value,String[] constraints,boolean leaf,Node Sibling)
		{
			this.Name = name;
			this.value = value;
			this.constraints = constraints;
			this.leaf = leaf;
			this.sibling = Sibling;
		}
		
		public Node(int typeCode,Node leftChild)
		{
			this.typeCode = typeCode;
			this.leftChild = leftChild;
		}
		
		public Node(String name,Node leftChild)
		{
			this.Name = name;
			this.leftChild = leftChild;
		}
		
		public Node(String name,int typeCode, byte[] value,boolean leaf, Node leftChild, Node sibling)
		{
			this.Name = name;
			this.typeCode = typeCode;
			this.value = value;
			this.leaf = leaf;
			this.leftChild = leftChild;
			this.sibling = sibling;
		}
		
		// set-get methods
		public String getName()
		{
			return this.Name;
		}

		public String[] getCons()
		{
			return this.constraints;
		}
		
		public int getCode()
		{
			return this.typeCode;
		}
		
		public byte[] getVal()
		{
			return this.value;
		}
		
		public boolean getLeaf()
		{
			return this.leaf;
		}
		
		public Node getLeft()
		{
			return this.leftChild;
		}
		
		public Node getSibl()
		{
			return this.sibling;
		}

		public void setName(String name)
		{
			this.Name = name;
		}
		
		public void setCode(int code)
		{
			this.typeCode = code;
		}
		
		public void setVal(byte[] val)
		{
			this.value = val;
		}
		
		public void setLeaf(boolean leaf)
		{
			this.leaf = leaf;
		}
		
		public void setLeft(Node left)
		{
			this.leftChild = left;
		}
		
		public void setSibl(Node sibl)
		{
			this.sibling = sibl;
		}

		public void setCons(String[] cons)
		{
			this.constraints = cons;
		}

	}
	public CommandTree(String cmd) throws Exception
	{
		cmd = cmd.trim();
		if (!cmd.endsWith(";"))
			throw new Exception("Incorrect Syntax! every sql statement should terminates with ;");
		parseCmd(cmd);
	}
	
	public Node getRoot()
	{
		return this.root;
	}
	
	private int checkType(String cmd)
	{
		String startStr = cmd.substring(0,cmd.indexOf(" "));
		switch(startStr){
		case "show":
			if (cmd.startsWith("show tables"))
				return CommandType.SHOW_TABLES.getCode();
			else
				return CommandType.SHOW_SCHEMAS.getCode();
		case "update":
			return CommandType.UPDATE.getCode();
		case"insert":
			return CommandType.INSERT_INTO_TABLE.getCode();
		case "select":
			return CommandType.SELECT.getCode();
		case "delete":
			return CommandType.DELETE_FROM.getCode();
		case "create":
			startStr = cmd.substring(0,cmd.indexOf(" ",7));
			if (startStr.equals("create table"))
			{
				return CommandType.CREATE_TABLE.getCode();
			}
			else if (startStr.equals("create database"))
			{
				return CommandType.CREATE_DATABASE.getCode();
			}
			else
			{
				return CommandType.CREATE_INDEX.getCode();
			}
		
		case "drop":
			startStr = cmd.substring(0,cmd.indexOf(" ", 5));
			if (startStr.equals("drop table"))
			{
				return CommandType.DROP_TABLE.getCode();
			}
			else
			{
				return CommandType.DROP_INDEX.getCode();
			}
		case "use":
			return CommandType.USE.getCode();
		}
		return -1;
	}
	
	
	private String getTarget(int typeCode,String cmd)
	{
		String name = null;
		int startIndex = -1;
		int endIndex = -1;
		switch(typeCode)
		{
		case 0:
			break;
		case 1:
			startIndex = cmd.indexOf(" ",7)+1;
			endIndex = cmd.indexOf(" ",startIndex);
			name = cmd.substring(startIndex, endIndex).trim();
			break;
		case 2:
			startIndex = cmd.indexOf(" ",7)+1;
			endIndex = cmd.indexOf(" ",startIndex);
			name = cmd.substring(startIndex, endIndex).trim();
			break;
		case 3:
			startIndex = cmd.indexOf(" ",5)+1;
			endIndex = cmd.indexOf(";",startIndex);
			name = cmd.substring(startIndex,endIndex).trim();
			break;
		case 4:
			startIndex = cmd.indexOf(" ",5)+1;
			endIndex = cmd.indexOf(";",startIndex);
			name = cmd.substring(startIndex,endIndex).trim();
			break;
		case 5:
			startIndex = cmd.indexOf(" ",10)+1;
			endIndex = cmd.indexOf(" ",startIndex);
			name = cmd.substring(startIndex, endIndex).trim();
			break;
		case 6:
			startIndex = cmd.indexOf(" ",7)+1;
			endIndex = cmd.indexOf(" ",startIndex);
			name = cmd.substring(startIndex,endIndex).trim();
			break;
		case 7:
			startIndex = cmd.indexOf(" ",0)+1;
			endIndex = cmd.indexOf(" ",startIndex);
			name = cmd.substring(startIndex, endIndex).trim();
			break;
		case 8:
			int fromIndex = cmd.indexOf("from");
			startIndex = cmd.indexOf(" ", fromIndex)+1;
			if (cmd.contains("where"))
				endIndex =  cmd.indexOf(" ",startIndex);
			else
				endIndex = cmd.indexOf(";",startIndex);
			name = cmd.substring(startIndex, endIndex).trim();
			break;		
		case 9:
			startIndex = cmd.indexOf("database")+9;
			name = cmd.substring(startIndex,cmd.indexOf(";")).trim();
			break;
		case 10:
			startIndex = cmd.indexOf("use")+4;
			name = cmd.substring(startIndex,cmd.indexOf(";")).trim();
			break;
		case 11:
			break;
		}		
		return name;
	}
	
	private Node[] checkAttr(int typeCode,String cmd) throws Exception
	{
		Node[] attr = null;
		switch(typeCode)
		{
		case 0:
			break;
		case 1:
			String attrList = cmd.substring(cmd.indexOf("(")+1,cmd.indexOf(");"));
			String[] attrs = attrList.split(",");
			attr = new Node[]{this.parseAttrs(attrs)};
			break;
		case 2:
			String table_col = cmd.substring(cmd.indexOf("on")+3,cmd.indexOf(")")+1);
			attr = new Node[]{this.parseIndex(table_col)};
			break;
		case 3:
			break;
		case 4:
			break;
		case 5:
			attr = this.parseInsert(cmd);
			break;
		case 6:
			attr = new Node[]{null,this.parseDelete(cmd)};
			break;
		case 7:
			attr = this.parseUpdate(cmd);
			break;
		case 8:
			attr = this.parseSelect(cmd);
			break;
		case 9:
			break;
		case 10:
			break;
		case 11:
			break;
		}	
		return attr;
	}
	
	private Node[] parseSelect(String cmd) throws Exception
	{
		String subStr = null;
		subStr = cmd.substring(cmd.indexOf("select")+7,cmd.indexOf("from")).trim();
		Node attrRoot = null;
		Node current = null;
		
		
		if(subStr.equals("*"))
			attrRoot = new Node("*",null,null,true,null);
		else
		{
			String[] list = subStr.split(",");
			for (int i=0;i<list.length;i++)
			{
				list[i]=list[i].trim();
				Node tmp = new Node(list[i],null,null,true,null);
				if(current != null)
					current.setSibl(tmp);
				if(i == 0)
					attrRoot = current = tmp;
				else
					current = tmp;
			}
		}
		Node whereRoot = parseDelete(cmd);
		return new Node[]{attrRoot,whereRoot};
	}
	
	private Node[] parseUpdate(String cmd) throws Exception
	{	
		String subStr = null;
		if (cmd.contains("where"))
			subStr = cmd.substring(cmd.indexOf("set")+4,cmd.indexOf("where")).trim();
		else
			subStr = cmd.substring(cmd.indexOf("set")+4,cmd.lastIndexOf(";"));
			
		String[] list = subStr.split(",");
		Node attrRoot = null;
		Node whereRoot = null;
		Node current = null;
		for (int i=0;i<list.length;i++)
			list[i] = list[i].trim();
		for (int i=0;i<list.length;i++)
		{
			String[] tmp = list[i].split("=");
			for (int j=0;j<tmp.length;j++)
				tmp[j] = tmp[j].trim();
			Node tmp1 = new Node(tmp[0],tmp[1].getBytes(),null,true,null);
			if(current != null)
				current.setSibl(tmp1);
			if(i == 0)
				attrRoot = current = tmp1;
			else
				current = tmp1;
		}
		whereRoot = parseDelete(cmd);
		return new Node[]{attrRoot,whereRoot};
	}
	
	private Node parseDelete(String cmd) throws Exception
	{
		if (!cmd.contains("where"))
			return null;
		else
		{
			String subStr = cmd.substring(cmd.indexOf("where")+6,cmd.lastIndexOf(";"));
			String[] list = null;
			String columnName = null;
			String operator = null;
			String value = null;
			if (subStr.contains("("))
			{
				String tmp1 = subStr.substring(0,subStr.indexOf("(")).trim();
				String[] tmp = tmp1.split(" ");
				columnName = tmp[0].trim();
				operator = tmp[1].trim();
				tmp1 = subStr.substring(subStr.indexOf("(")+1,subStr.indexOf(")"));
				list = tmp1.split(",");
				for (int i=0;i<list.length;i++)
					list[i] = list[i].trim();
			}
			else
			{
				Pattern patt = Pattern.compile("[<>=!]{1,2}");
				Matcher matcher = patt.matcher(subStr);
				if(matcher.find()){
					operator = matcher.group().trim();
				}
				else
					throw new Exception("Error 04: you have an error in your SQL syntax, please check it first!");
				String[] tmp = subStr.split(operator);
				columnName = tmp[0].trim();
				value = tmp[1].trim();
			}
			
			
			Node root = null;
			Node current = null;
			
			if(list == null)
				return new Node(columnName,value.getBytes(),new String[]{operator},true,null);
			else
			{
				for (int i=0;i<list.length;i++)
				{
					if (operator.equals("in"))
						operator = "=";
					Node tmp = new Node(columnName,list[i].getBytes(),new String[]{operator},true,null);
					if (current != null)
						current.setSibl(tmp);
					if (i == 0)
						root = current = tmp;
					else
						current = tmp;
				}	
			}
			return root;
		}
	}
	
	private Node[] parseInsert(String cmd) throws Exception
	{
		Node root = null;
		Node current = null;
		String subStr = cmd.substring(0,cmd.indexOf("values")).trim();
		String[] columnList = null;
		if (subStr.contains("(") && subStr.contains(")"))
		{
			String columns = subStr.substring(subStr.indexOf("(")+1,subStr.indexOf(")"));
			columnList = columns.split(",");
			for (int i=0;i<columnList.length;i++)
				columnList[i] = columnList[i].trim();
		}
		else if ((subStr.contains("(") && !subStr.contains(")")) || (!subStr.contains("(") && subStr.contains(")")))
			throw new Exception("Error 04: you have an error in your SQL syntax, please check it first!");
		subStr = cmd.substring(cmd.indexOf("(",cmd.indexOf("values"))+1, cmd.lastIndexOf(")")).trim();
		String[] valueList = subStr.split(",");
		for (int i=0;i<valueList.length;i++)
		{	
			valueList[i] = valueList[i].trim();
			Node tmp = null;
			if (columnList != null)
				tmp = new Node(columnList[i],valueList[i].getBytes(),null,true,null);
			else
				tmp = new Node(null,valueList[i].getBytes(),null,true,null);
			
			if (current != null)
				current.setSibl(tmp);
			
			if (i == 0)
				root = current = tmp;
			else
				current = tmp;	
		}
		Node whereRoot = parseDelete(cmd);
		return new Node[]{root,whereRoot};
		
	}
	
	private Node parseIndex(String table_col) throws Exception
	{
		Node attr = null;
		String tableName = table_col.substring(0,table_col.indexOf("(")).trim();
		String columnName = table_col.substring(table_col.indexOf("(")+1,table_col.indexOf(")"));
		
		// for index, operation_target = index_name  attr_name = table_name attr_constraint = column_name
		attr = new Node(tableName,null,new String[]{columnName},true,null);
		return attr;
	}
	private Node parseAttrs(String[] attrs) throws Exception
	{
		Node root = null;
		Node current = null;
		int priCount = 0;
		for (int i=0;i<attrs.length;i++)
		{
			attrs[i] = attrs[i].trim();
			
			
			String[] tmp = attrs[i].split(" ");
			
			
			Node tmp2 = null;
			
			if (tmp.length == 2)
			{
				tmp2 = new Node(tmp[0],null,new String[]{tmp[1]},true,null);
			}
			else
			{
				if (attrs[i].contains("primary key") && attrs[i].contains("not null"))
				{
					tmp2 = new Node(tmp[0],null,new String[]{tmp[1],"pri","not null"},true,null);
					priCount++;
				}
				else if (attrs[i].contains("primary key"))
				{
					tmp2 = new Node(tmp[0],null,new String[]{tmp[1],"pri"},true,null);
					priCount++;
				}
				else if (attrs[i].contains("not null"))
				{
					tmp2 = new Node(tmp[0],null,new String[]{tmp[1],"not null"},true,null);
				}
			}
			
			if(priCount>1)
				throw new Exception("Integrity constraint violation is found (primary key constraint is specified on multiple columns)");
			
			if (current != null)
				current.setSibl(tmp2);
			
			if (i==0)
				root = current = tmp2;
			else
				current = tmp2;
		}
		return root;
	}
	
	public void parseCmd(String cmd) throws Exception
	{
		int typeCode = checkType(cmd);
		String tarName = getTarget(typeCode,cmd);
		Node[] attr = checkAttr(typeCode,cmd);
		constructTree(typeCode,tarName,attr);
	}
	
	private void constructTree(int typeCode,String target,Node[] attr)
	{
		Node attrRoot = null;
		Node whereRoot = null;
		

		if(attr!=null)
		{
		
			if(attr.length == 1)
			{
				attrRoot = attr[0];
			}
			else
			{
				attrRoot = attr[0];
				whereRoot = attr[1];
			}
		}
		Node opeTarg = new Node(target,attrRoot);
		if (whereRoot!=null)
			opeTarg.setSibl(whereRoot);
		this.root = new Node(typeCode,opeTarg);
	}
	
	public void printTree()
	{
		System.out.println(this.root.getCode());
		System.out.print(this.root.getLeft().getName());
		if(this.root.getLeft().getSibl()!=null)
		{
			Node sib = this.root.getLeft().getSibl();
			System.out.print("------where--------->");
			while(sib != null){
				System.out.print(sib.getName()+sib.getCons()[0]+ new String(sib.getVal()));
				System.out.print(",");
				sib = sib.getSibl();
			}
		}
		System.out.println();
		Node attrRoot = this.root.getLeft().getLeft();
		boolean select = false;
		if (this.root.getCode() == 8)
			select = true;
			
		while(attrRoot!=null){
			if(attrRoot.getVal()!=null)
				System.out.print(attrRoot.getName()+"("+ new String(attrRoot.getVal())+")"+", ");
			if(attrRoot.getCons()!=null){
				System.out.print(attrRoot.getName()+" (");
				for (int i=0;i<attrRoot.getCons().length;i++)
					if(attrRoot.getCons()[i]!=null)
						System.out.print(" "+attrRoot.getCons()[i]+" ");
				System.out.print(") -> ");
			}
			if(select)
				System.out.print(attrRoot.getName()+" -> ");
			attrRoot = attrRoot.getSibl();
		}
		
		System.out.println();
	}
}
