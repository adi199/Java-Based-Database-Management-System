package edu.ottawa;

import edu.ottawa.extensions.SetupDatabase;

import java.util.Scanner;

/**
 *  @author Chris Irwin Davis
 *  @version 1.0
 *  <b>
 *  <p>This is an example of how to create an interactive prompt</p>
 *  <p>There is also some guidance to get started with read/write of
 *     binary data files using the RandomAccessFile class from the
 *     Java Standard Library.</p>
 *  </b>
 *
 */
public class DavisBase {

	/* 
	 *  The Scanner class is used to collect user commands from the prompt
	 *  There are many ways to do this. This is just one.
	 *
	 *  Each time the semicolon (;) delimiter is entered, the userCommand 
	 *  String is re-populated.
	 */
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
	/** ***********************************************************************
	 *  Main method
	 */
    public static void main(String[] args) {
		SetupDatabase.initDB();
		/* Display the welcome screen */
		Utils.splashScreen();

		/* Variable to hold user input from the prompt */
		String userCommand = ""; 

		while(!Settings.isExit()) {
			System.out.print(Settings.getPrompt());
			/* Strip newlines and carriage returns */
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim();
			Commands.parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");
	}
}
