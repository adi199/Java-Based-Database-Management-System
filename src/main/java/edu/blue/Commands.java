package edu.blue;

import edu.blue.extensions.helpers.*;

import java.util.ArrayList;
import java.util.Arrays;

import static java.lang.System.out;

public class Commands {

    /* This method determines what type of command the userCommand is and
     * calls the appropriate method to parse the userCommand String.
     */
    public static void parseUserCommand(String userCommand) {

        /* Clean up command string so that each token is separated by a single space */
        userCommand = userCommand.replaceAll("\n", " ");    // Remove newlines
        userCommand = userCommand.replaceAll("\r", " ");    // Remove carriage returns
        /*userCommand = userCommand.replaceAll(",", " , ");   // Tokenize commas
        userCommand = userCommand.replaceAll("\\(", " ( "); // Tokenize left parentheses
        userCommand = userCommand.replaceAll("\\)", " ) "); // Tokenize right parentheses
        userCommand = userCommand.replaceAll("( )+", " ");  // Reduce multiple spaces to a single space*/

        /* commandTokens is an array of Strings that contains one lexical token per array
         * element. The first token can be used to determine the type of command
         * The other tokens can be used to pass relevant parameters to each command-specific
         * method inside each case statement
         */
        ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

        /*
         *  This switch handles a very small list of hard-coded commands from SQL syntax.
         *  You will want to rewrite this method to interpret more complex commands.
         */
        switch (commandTokens.get(0).toLowerCase()) {
            case "show":
                System.out.println("Case: SHOW");
                show(commandTokens);
                break;
            case "select":
                System.out.println("Case: SELECT");
                parseQuery(commandTokens);
                break;
            case "create":
                System.out.println("Case: CREATE");
                parseCreateTableOrIndex(userCommand);
                break;
            case "insert":
                System.out.println("Case: INSERT");
                parseInsert(commandTokens);
                break;
            case "delete":
                System.out.println("Case: DELETE");
                parseDelete(commandTokens);
                break;
            case "update":
                System.out.println("Case: UPDATE");
                parseUpdate(commandTokens);
                break;
            case "drop":
                System.out.println("Case: DROP");
                dropTable(commandTokens);
                break;
            case "help":
                help();
                break;
            case "version":
                displayVersion();
                break;
            case "exit":
                Settings.setExit(true);
                break;
            case "quit":
                Settings.setExit(true);
                break;
            default:
                System.out.println("I didn't understand the command: \"" + userCommand + "\"");
                break;
        }
    }

    public static void displayVersion() {
        System.out.println("DavisBaseLite Version " + Settings.getVersion());
        System.out.println(Settings.getCopyright());
    }

    public static void parseCreateTableOrIndex(String command) {
        /* TODO: Before attempting to create new table file, check if the table already exists */

        System.out.println("Stub: parseCreateTable method");
        System.out.println("Command: " + command);
        ArrayList<String> commandTokens = commandStringToTokenList(command);

        /* Extract the table name from the command string token list */
        String identifier = commandTokens.get(1);

        if (identifier.equalsIgnoreCase("index")) {
            IndexCreationHelper.performIndexCreation(command);
            return;
        }

        /* YOUR CODE GOES HERE */

        /*  Code to create a .tbl file to contain table data */
        try {
            CreateTableHelper.performCreateTableOperation(command);
            /*  Create RandomAccessFile tableFile in read-write mode.
             *  Note that this doesn't create the table file in the correct directory structure
             */

            /* Create a new table file whose initial size is one page (i.e. page size number of bytes) */
            /*RandomAccessFile tableFile = new RandomAccessFile("data/user_data/" + tableFileName, "rw");
            tableFile.setLength(Settings.getPageSize());

            *//* Write page header with initial configuration *//*
            tableFile.seek(0);
            tableFile.writeInt(0x0D);       // Page type
            tableFile.seek(0x02);
            tableFile.writeShort(0x01FF);   // Offset beginning of cell content area
            tableFile.seek(0x06);
            tableFile.writeInt(0xFFFFFFFF); // Sibling page to the right
            tableFile.seek(0x0A);
            tableFile.writeInt(0xFFFFFFFF); // Parent page*/
        } catch (Exception e) {
            System.out.println(e);
        }

        /*  Code to insert an entry in the TABLES meta-data for this new table.
         *  i.e. New row in davisbase_tables if you're using that mechanism for meta-data.
         */

        /*  Code to insert entries in the COLUMNS meta data for each column in the new table.
         *  i.e. New rows in davisbase_columns if you're using that mechanism for meta-data.
         */
    }

    public static void show(ArrayList<String> commandTokens) {
        System.out.println("Command: " + tokensToCommandString(commandTokens));
        System.out.println("Stub: This is the show method");
        /* TODO: Your code goes here */
        parseQuery(commandStringToTokenList("select * from davisbase_tables"));
    }

    /*
     *  Stub method for inserting a new record into a table.
     */
    public static void parseInsert(ArrayList<String> commandTokens) {
        System.out.println("Command: " + tokensToCommandString(commandTokens));
        System.out.println("Stub: This is the insertRecord method");
        /* TODO: Your code goes here */
        InsertOperationHelper.performInsertOperation(tokensToCommandString(commandTokens));
    }

    public static void parseDelete(ArrayList<String> commandTokens) {
        System.out.println("Command: " + tokensToCommandString(commandTokens));
        System.out.println("Stub: This is the deleteRecord method");
        /* TODO: Your code goes here */
        DeleteOperationHelper.performDeleteOperation(tokensToCommandString(commandTokens));
    }


    /**
     * Stub method for dropping tables
     */
    public static void dropTable(ArrayList<String> commandTokens) {
        System.out.println("Command: " + tokensToCommandString(commandTokens));
        System.out.println("Stub: This is the dropTable method.");
        DropOperationHelper.dropTable(tokensToCommandString(commandTokens));
    }

    /**
     * Stub method for executing queries
     */
    public static void parseQuery(ArrayList<String> commandTokens) {
        System.out.println("Command: " + tokensToCommandString(commandTokens));
        System.out.println("Stub: This is the parseQuery method");
        SelectOperationHelper.performSelectOperation(tokensToCommandString(commandTokens));
    }

    /**
     * Stub method for updating records
     *
     * @param commandTokens is a arraylsit of the user input
     */
    public static void parseUpdate(ArrayList<String> commandTokens) {
        System.out.println("Command: " + tokensToCommandString(commandTokens));
        System.out.println("Stub: This is the performUpdateOperation method");
        UpdateOperationHelper.performUpdateOperation(tokensToCommandString(commandTokens));
    }

    public static String tokensToCommandString(ArrayList<String> commandTokens) {
        String commandString = "";
        for (String token : commandTokens)
            commandString = commandString + token + " ";
        return commandString;
    }

    public static ArrayList<String> commandStringToTokenList(String command) {
        command.replace("\n", " ");
        command.replace("\r", " ");
        ArrayList<String> tokenizedCommand = new ArrayList<String>(Arrays.asList(command.split(" ")));
        return tokenizedCommand;
    }

    /**
     * Help: Display supported commands
     */
    public static void help() {
        out.println(Utils.printSeparator("*", 80));
        out.println("SUPPORTED COMMANDS\n");
        out.println("All commands below are case insensitive\n");
        out.println("SHOW TABLES;");
        out.println("\tDisplay the names of all tables.\n");
        out.println("SELECT ⟨column_list⟩ FROM table_name [WHERE condition];\n");
        out.println("\tDisplay table records whose optional condition");
        out.println("\tis <column_name> = <value>.\n");
        out.println("INSERT INTO (column1, column2, ...) table_name VALUES (value1, value2, ...);\n");
        out.println("\tInsert new record into the table.");
        out.println("UPDATE <table_name> SET <column_name> = <value> [WHERE <condition>];");
        out.println("\tModify records data whose optional <condition> is\n");
        out.println("DROP TABLE table_name;");
        out.println("\tRemove table data (i.e. all records) and its schema.\n");
        out.println("VERSION;");
        out.println("\tDisplay the program version.\n");
        out.println("HELP;");
        out.println("\tDisplay this help information.\n");
        out.println("EXIT;");
        out.println("\tExit the program.\n");
        out.println(Utils.printSeparator("*", 80));
    }

}
