package davisDB;
import java.io.*;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;


import java.security.*;
import java.util.Base64;

public class CmdPrompt {
	private String username = null;
	private KeyStore.PasswordProtection protectedPW = null;
	private Console cons = System.console();
	private String path = null;
	private String args = null;

	

	public CmdPrompt(String args,String path){
		this.path = path;
		this.args = args;
		this.createAccount();
	}
	
		
	private void parseInput(String args){
		char[] passwd = null;
		try{
			if (args.contains("-u") && !args.contains("-p")){
				
				System.out.printf("ERROR 01: Access denied for user '%s'@'localhost' (using password: NO)", (Object[])new String[]{args.substring(args.indexOf("-u")+3)});
				System.exit(-1);
			}
			if (!args.contains("-u") && args.contains("-p")){
				System.out.println("ERROR 02: Access denied (Missing Username)"); 
				System.exit(-2);
			}
			if (args.contains("-u") && args.contains("-p")){
				this.username = args.substring(args.indexOf("-u")+3,args.indexOf("-p"));
				if (cons!=null)
					passwd = cons.readPassword("[%s:]",(Object[])new String[]{"Password:"}); 
			}
			if (!args.contains("-u") && !args.contains("-p")){
				if (cons!=null)
					this.username = cons.readLine("%s: ", (Object[])new String[]{"Username"});
					passwd = cons.readPassword("%s: ", (Object[])new String[]{"Password"});
			}
			this.protectedPW = new KeyStore.PasswordProtection(passwd);
			java.util.Arrays.fill(passwd, ' ');
		}
		catch(Exception e){
			System.out.println(e.toString());
			e.printStackTrace();
		}
	}
	private void secureSaveKey(String username,char[] passwd){
		try
		{
			KeyStore ks = KeyStore.getInstance("jceks");
			File keyFile = new File(this.path);			
			java.io.FileInputStream fis = null;
			if (keyFile.exists()){
				fis = new FileInputStream(keyFile);
			}
			else{
				fis = null;
			}
			javax.crypto.SecretKey mySecretKey = null;
			try
			{
				ks.load(fis,passwd);
				if (fis == null){
					PBEKeySpec pbeKeySpec = new PBEKeySpec(passwd);
					SecretKeyFactory keyFac = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
					mySecretKey = keyFac.generateSecret(pbeKeySpec);
					
					// clear password
					pbeKeySpec.clearPassword();
				}
				else if(ks.containsAlias(username)){
					System.out.println("User already exists!");
					return;
				}
				else{
					PBEKeySpec pbeKeySpec = new PBEKeySpec(passwd);
					SecretKeyFactory keyFac = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
					mySecretKey = keyFac.generateSecret(pbeKeySpec);
					
					// clear password
					pbeKeySpec.clearPassword();
				}
			}
			finally
			{
				if (fis!=null)
					fis.close();
			}
			// save my secret key
			KeyStore.ProtectionParameter protParam = new KeyStore.PasswordProtection(passwd);
			KeyStore.SecretKeyEntry skEntry = new KeyStore.SecretKeyEntry(mySecretKey);
			ks.setEntry(username, skEntry, protParam);
			
			// store away the keyStore
			java.io.FileOutputStream fos = null;
			try{
				fos = new java.io.FileOutputStream("secureKeyStore");
				ks.store(fos,passwd);
				java.util.Arrays.fill(passwd, ' ');
			}
			finally{
				if (fos!=null){
					fos.close();
				}
			}
		}
		catch(Exception e)
		{
			System.out.println(e.toString());
			e.printStackTrace();
		}
	}
	
	private void logIn(){
		try
		{
			KeyStore ks = KeyStore.getInstance("jceks");
			File keyFile = new File(this.path);
			java.io.FileInputStream fis = null;
			char[] passwd = this.protectedPW.getPassword();
			try
			{
				fis = new FileInputStream(keyFile);
				ks.load(fis,passwd);
				KeyStore.ProtectionParameter protParam = new KeyStore.PasswordProtection(passwd);
				KeyStore.SecretKeyEntry entry = (KeyStore.SecretKeyEntry)ks.getEntry(this.username, protParam);
				if (entry != null){
					PBEKeySpec pbeKeySpec = new PBEKeySpec(passwd);
					SecretKeyFactory keyFac = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
					SecretKey pbeKey = keyFac.generateSecret(pbeKeySpec);
					SecretKey mySecretKey = entry.getSecretKey();
					
					// clear password
					pbeKeySpec.clearPassword();
					
					// convert key to string
					String pbeKeyStr = Base64.getEncoder().encodeToString(pbeKey.getEncoded());
					String mySecretKeyStr = Base64.getEncoder().encodeToString(mySecretKey.getEncoded());
					
					if (mySecretKeyStr.equals(pbeKeyStr)){
						this.start();
					}
				}
				else{
					System.out.println("Cannot find this user!");
				}
			}
			catch(java.io.IOException e){
				System.out.println("Password Incorrect!\nSystem exits!");
				System.exit(-3);
			}
			finally
			{
				if(fis != null){
					fis.close();
				}
				// clear password
				java.util.Arrays.fill(passwd,' ');
			}

		}
		catch(Exception e){
			System.out.println(e.toString());
			e.printStackTrace();
		}
	}
	private void createAccount(){
		File keyFile = new File(this.path);
		if (keyFile.exists()){
			this.parseInput(this.args);
			this.logIn();
		}
		else{
			System.out.println("This is your first time using Davisql, please create your account");
			this.parseInput(this.args);
			this.secureSaveKey(this.username, this.protectedPW.getPassword().clone());
			this.start();	
		}
	}

	public void start(){
		this.welcome();
		new Terminal().start();
	}
	
	private void welcome(){
		System.out.println("\nWelcome to DavisDB!");
		System.out.println("Current Time:  "+java.time.LocalDateTime.now().toLocalDate()+" "+java.time.LocalDateTime.now().toLocalTime());
		System.out.println("Type 'help;' for help.\n");
		// clear userName
		this.username=null;
	}
	
	public static void main(String[] args){
		System.out.println("Please log into Davisql database first!");
	    String cwd = System.getProperty("user.dir");
		String path = cwd.concat("/secureKeyStore");
		@SuppressWarnings("unused")
		CmdPrompt cmd = new CmdPrompt("Davisql",path);
	}
}
