package davisDB;
import java.util.Scanner;

public class Terminal {
	private static String prompt = "DavisDB> ";
	private CommandExecutor cmdExecutor = new CommandExecutor();

	public void start()
	{
		System.out.print(prompt);
		String userCmdInput = "";
		Scanner reader = new Scanner(System.in);
		try
		{
		
			while(!userCmdInput.equals("exit;"))
			{
				boolean tokenComp = true;
				boolean multiline = true;
				
				String token = reader.next();				
				
				token = token.replace('\n',' ').trim().replace('\r', ' ').trim().toLowerCase();
			
				if (token.endsWith("(") || token.endsWith(")") || token.endsWith(","))
					multiline = false;
				
				if ("version".contains(token) || "help".contains(token) || "exit".contains(token))
					tokenComp = false;	
							
				if (token.endsWith(";"))
				{
					userCmdInput += token;
					if (!userCmdInput.equals("exit;"))
					{
						try
						{
							executeCmd(userCmdInput);
						}
						catch(Exception e){
							System.out.println(e.toString().substring(e.toString().indexOf(':')+1));
						}
						userCmdInput = "";
						System.out.print(prompt);
					}
					else
					{
						System.out.println("Bye!");
					}
				}
				else
				{
					if (!tokenComp)
					{
						userCmdInput += token;
						System.out.print("      -> ");
					}
					else
					{
						userCmdInput += (token+' ');
						if (!multiline)
							System.out.print("      -> ");
					}
					if(token.equals(""))
						System.out.print(prompt);
				}
			}
		}
		finally
		{
			reader.close();
		}
	}
	
	public void executeCmd(String cmd) throws Exception
	{
		cmd = cmd.trim();
		String startStr = null;
		if (cmd.contains(" "))
			startStr = cmd.substring(0,cmd.indexOf(' '));
		else
			startStr = cmd;
		switch (startStr){
		case "help;":
			help();
			break;
		case "version;":
			version();
			break;
		case "create":
			create(cmd);
			break;
		case "show":
			show(cmd);
			break;
		case "insert":
			insert(cmd);
			break;
		case "delete":
			delete(cmd);
			break;
		case "update":
			update(cmd);
			break;
		case "select":
			select(cmd);
			break;
		case "use":
			use(cmd);
			break;
		case "drop":
			drop(cmd);
			break;
		default:
			System.out.println("ERROR 03: Unsupported Command! Type 'help;' to check supported commands!\n");
		}
	}
	

	public static void help()
	{
		System.out.println();
		System.out.println("List of all DavisDB commands:");
		System.out.println("Note that all text commands must be first on line and end with ';'");
		System.out.println("All commands are CASE INSENSITIVE!");
		System.out.println("DDL:");
		System.out.println("        SHOW TABLES -- Display a list of all tables in DavisDB");
		System.out.println("        CREATE TABLE -- Create a new table schema,i.e. a new empty table");
		System.out.println("        CREATE INDEX -- Create a new table index");
		System.out.println("        DROP TABLE -- Remove a table schema, and all of its contained data.");
		System.out.println("        DROP INDEX -- Remove a table index");
		System.out.println("DML:");
		System.out.println("        INSERT INTO TABLE -- Inserts a single record into a table");
		System.out.println("        DELETE FROM -- Deletes one or more records from a table");
		System.out.println("        UPDATE -- Modifies one or more records in a table");
		System.out.println("VDL:");
		System.out.println("        SELECT-FROM-WHERE -- style query");
		System.out.println("        EXIT -- Cleanly exits the database and saves all table and index information in non-volatile files");
		System.out.println();
	}
	
	public void version()
	{
		System.out.println("SQL DavisDB v1.0");
		System.out.println();
	}
	
	public void create(String cmd) throws Exception
	{		
		CommandTree cTree = new CommandTree(cmd);
		//cTree.printTree();
		System.out.println();
		this.cmdExecutor.execute(cTree);
		System.out.println("\n");
		
	}
    
	public void show(String cmd) throws Exception 
	{ 
		CommandTree cTree = new CommandTree(cmd);
		System.out.println();
		this.cmdExecutor.execute(cTree);
		//cTree.printTree();
		System.out.println("\n");

	}
	
	public void insert(String cmd) throws Exception
	{
		CommandTree cTree = new CommandTree(cmd);
		System.out.println();
		this.cmdExecutor.execute(cTree);
		//cTree.printTree();
		System.out.println("\n");

		
	}
	
	public void delete(String cmd) throws Exception
	{
		
		CommandTree cTree = new CommandTree(cmd);
		System.out.println();
		this.cmdExecutor.execute(cTree);
		//cTree.printTree();
		System.out.println("\n");

	}
	
	public void update(String cmd) throws Exception
	{
		CommandTree cTree = new CommandTree(cmd);
		System.out.println();
		this.cmdExecutor.execute(cTree);
		//cTree.printTree();
		System.out.println("\n");

		
	}
	
	public void select(String cmd) throws Exception
	{
		CommandTree cTree = new CommandTree(cmd);
		System.out.println();
		this.cmdExecutor.execute(cTree);
		//cTree.printTree();
		System.out.println("\n");

	}
	
	public void use(String cmd) throws Exception
	{
		CommandTree cTree = new CommandTree(cmd);
		System.out.println();
		this.cmdExecutor.execute(cTree);
		//cTree.printTree();
		System.out.println("\n");

	}
	
	private void drop(String cmd) throws Exception {
		CommandTree cTree = new CommandTree(cmd);
		System.out.println();
		this.cmdExecutor.execute(cTree);
		//cTree.printTree();
		System.out.println("\n");

	}
	
}
