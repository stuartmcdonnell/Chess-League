package chessleague;

import java.sql.*;
import java.io.*;
import java.util.*;

class ChessLeague {

    Connection conn;

    public static void main(String args[])
            throws SQLException, IOException {

        ChessLeague chess_league = new ChessLeague();
        chess_league.start();

    }

    private void start() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("Could not load the driver");
        }

        String user, pass, host;

        //Asks for information required to make a connection to the database.
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter Your Username:");
        user = sc.next();
        System.out.println("Now Enter Your Password:");
        pass = sc.next();
        System.out.println("Now Enter Hostname:");
        host = sc.next();

        //  userid, password and hostname are obtained from the console
        try {
            conn = DriverManager.getConnection("jdbc:mysql://"+host+":3306", user, pass);
            conn.setAutoCommit(false);
            System.out.println("Connected!");
            
            //Creates the database if it hasn't already been, will use modified connection URL until database has been created.
            try {
                Statement s = conn.createStatement();
                //Create Database
                s.execute("CREATE DATABASE IF NOT EXISTS `chess_league`");
                conn.commit();
                System.out.println("Database Created!");
                
                conn = DriverManager.getConnection("jdbc:mysql://" + host + ":3306/chess_league", user, pass);
                conn.setAutoCommit(false);
                
            } catch (Exception exc) {
                System.out.println("Error Creating Database: " + exc.toString());
            }
   
        } catch (Exception exc) {
            System.out.println(exc.toString());
        }
        
        

        //Startup Done
        try {
            startCmd();
            conn.close();
        } catch (Exception exc) {
            System.out.println(exc.toString());
        }

    }

    private void createTables() {
        try {
            Statement s = conn.createStatement();
            //Create Club
            s.execute("CREATE TABLE IF NOT EXISTS `club` (   `ClubName` varchar(10) NOT NULL,   `Address` varchar(15) NOT NULL,   `DateFormed` date NOT NULL ) ENGINE=InnoDB DEFAULT CHARSET=latin1;");
            //Create Game
            s.execute("CREATE TABLE IF NOT EXISTS `game` (   `GameID` int(11) NOT NULL,   `DatePlayed` date NOT NULL,   `BoardNum` int(11) NOT NULL,   `Score` varchar(11) NOT NULL,   `MatchID` int(11) NOT NULL,   `WhitePlayer` varchar(10) NOT NULL,   `BlackPlayer` varchar(10) NOT NULL ) ENGINE=InnoDB DEFAULT CHARSET=latin1;");
            //Create Match
            s.execute("CREATE TABLE IF NOT EXISTS `match` (   `MatchID` int(11) NOT NULL,   `MatchDate` date NOT NULL,   `Venue` varchar(20) NOT NULL,   `Score` varchar(11) NOT NULL,   `WinningClub` varchar(20) NOT NULL,   `LosingClub` varchar(20) NOT NULL ) ENGINE=InnoDB DEFAULT CHARSET=latin1;");
            //Create Player
            s.execute("CREATE TABLE IF NOT EXISTS `player` (   `PlayerName` varchar(10) NOT NULL,   `DateOfBirth` date NOT NULL,   `FIDERating` varchar(10) NOT NULL,   `FIDETitle` varchar(20) NOT NULL,   `ClubName` varchar(10) NOT NULL ) ENGINE=InnoDB DEFAULT CHARSET=latin1;");
            conn.commit();
            System.out.println("Tables Created!");
        } catch (Exception exc) {
            System.out.println("Error Creating Tables: " + exc.toString());
        }
    }

    private void addKeys() {
        try {
            Statement s = conn.createStatement();
            s.execute("ALTER TABLE `club`   ADD PRIMARY KEY (`ClubName`);");
            s.execute("ALTER TABLE `game`   ADD PRIMARY KEY (`GameID`);");
            s.execute("ALTER TABLE `match`   ADD PRIMARY KEY (`MatchID`);");
            s.execute("ALTER TABLE `player`   ADD PRIMARY KEY (`PlayerName`);");
            //Adding Foreign Keys
            s.execute("ALTER TABLE player ADD FOREIGN KEY (`ClubName`) REFERENCES club(`ClubName`);");
            s.execute("ALTER TABLE game ADD FOREIGN KEY (`MatchID`) REFERENCES chess_league.match(`MatchID`);");
            conn.commit();
        } catch (Exception exc) {
            System.out.println("Error Adding PK: " + exc.toString());
        }
    }

    private void loadData() {

        try {
            Statement s = conn.createStatement();
            s.execute("LOAD DATA INFILE \"club.txt\" REPLACE INTO TABLE club Fields terminated by \",\" Lines terminated by \"\\r\\n\"");
            s.execute("LOAD DATA INFILE \"players.txt\" REPLACE INTO TABLE player Fields terminated by \",\" Lines terminated by \"\\r\\n\"");
            s.execute("LOAD DATA INFILE \"games.txt\" REPLACE INTO TABLE game Fields terminated by \",\" Lines terminated by \"\\r\\n\"");
            s.execute("LOAD DATA INFILE \"matches.txt\" REPLACE INTO TABLE chess_league.match Fields terminated by \",\" Lines terminated by \"\\r\\n\"");

            conn.commit();
            System.out.println("Data Loaded!");
        } catch (Exception exc) {
            System.out.println("Error Loading Data: " + exc.toString());
        }

    }
    
    private void createProcedures() {
        //Creates each stored procedure, executed as queries during the setup phase, formated as SQL.
        try {
            Statement s = conn.createStatement();
            s.execute("CREATE PROCEDURE `getPlayerClub`(IN `desired_club` TEXT) NOT DETERMINISTIC CONTAINS SQL SQL SECURITY DEFINER BEGIN SELECT player.PlayerName, club.ClubName FROM player JOIN club ON player.ClubName = club.ClubName WHERE player.ClubName = desired_club; END");
            s.execute("CREATE PROCEDURE getPlayers() BEGIN SELECT * FROM player; END");
            s.execute("CREATE PROCEDURE countGrandmasters() BEGIN SELECT COUNT('Grandmasters') FROM player WHERE player.FIDETitle='Grandmaster'; END");
            s.execute("CREATE PROCEDURE getGrandmastersClub() BEGIN SELECT `PlayerName`, `ClubName` FROM `player` WHERE `FIDETitle` = 'Grandmaster' GROUP BY ClubName; END");
            s.execute("CREATE PROCEDURE `getByRating`(IN `fide` INT(10)) NOT DETERMINISTIC CONTAINS SQL SQL SECURITY DEFINER BEGIN SELECT * FROM player WHERE player.FIDERating > fide; END");
            s.execute("CREATE PROCEDURE `bestClubRating`(IN `club` TEXT) NOT DETERMINISTIC CONTAINS SQL SQL SECURITY DEFINER BEGIN SELECT * FROM player WHERE player.ClubName = club ORDER BY convert(player.FIDERating, decimal) DESC; END");
            s.execute("CREATE PROCEDURE `getClubsAvgFide`() NOT DETERMINISTIC CONTAINS SQL SQL SECURITY DEFINER SELECT club.ClubName,ROUND(AVG(player.FIDERating),2) avg_fide FROM club JOIN player ON club.ClubName = player.ClubName GROUP BY club.ClubName;");
            s.execute("CREATE PROCEDURE `getClubTotalPlayersOver`(IN `desired_players` INT(10)) NOT DETERMINISTIC CONTAINS SQL SQL SECURITY DEFINER SELECT club.ClubName,COUNT(player.PlayerName) AS total_players FROM club JOIN player ON club.ClubName = player.ClubName GROUP BY club.ClubName HAVING total_players > desired_players;");
            conn.commit();
        } catch (Exception exc) {
            System.out.println("Error Creating Procedures: ");System.out.println(exc.toString());
        }

    }


    private void startCmd() {

        Scanner sc = new Scanner(System.in);

        try {

            System.out.println("------------");
            System.out.println("Select from the following: (Input Number)");
            System.out.println("1. Run Setup");
            System.out.println("2. List all Players (Name | Rating | Club)");
            System.out.println("3. Get Players with X Rating");
            System.out.println("4. List All Players By Club Name");
            System.out.println("5. Count All Grandmasters in League");
            System.out.println("6. Players By Club Sorted By FIDE Rating");
            System.out.println("7. List Clubs by Average FIDE Rating");
            System.out.println("8. Select Groups With Number of Players Above X");
            System.out.println("Enter Selection Below...");

            int selection = Integer.parseInt(sc.nextLine());
            
            switch (selection) {
                case 1://Calls Setup functions to create structured database.
                    createTables();
                    addKeys();
                    loadData();
                    createProcedures();
                    startCmd();
                    break;
                    //The following cases each execute a stored procedure with various inputs and results.
                case 2:
                    CallableStatement cstmt_getPlayers = conn.prepareCall("{call getPlayers()}");
                    ResultSet rs1 = cstmt_getPlayers.executeQuery();
                    while(rs1.next()){
                        String out = ("Player Name: "+rs1.getString(1)+"         "+"Player Rating: "+rs1.getString(3)+"         Club Name: "+rs1.getString(5));
                        System.out.println(out);
                    }
                    startCmd();
                break;
                case 3:
                    CallableStatement cstmt_getByRating = conn.prepareCall("{call getByRating(?)}");
                    System.out.println("Please Enter Minimum FIDE Rating: ");
                    int min_rating = Integer.parseInt(sc.nextLine());
                    cstmt_getByRating.setInt("fide", min_rating);
                    ResultSet rs2 = cstmt_getByRating.executeQuery();
                    
                    while(rs2.next()){
                        String out2 = ("Player Name: "+rs2.getString(1)+"           FIDE Rating: "+rs2.getString(3)+"           FIDE Title: "+rs2.getString(4));
                        System.out.println(out2);
                    }
                    startCmd();
                break;
                case 4:
                    CallableStatement cstmt_getByClub = conn.prepareCall("{call getPlayerClub(?)}");
                    System.out.println("Please Enter Search By Club Name:");
                    cstmt_getByClub.setString("desired_club", sc.nextLine());
                    ResultSet rs3 = cstmt_getByClub.executeQuery();
                    
                    while(rs3.next()){
                        String out3 = ("Player Name: "+rs3.getString(1));
                        System.out.println(out3);
                    }
                    startCmd();
                    break;
                case 5:
                    CallableStatement cstmt_countGrandmasters = conn.prepareCall("{call countGrandmasters()}");
                    ResultSet rs4 = cstmt_countGrandmasters.executeQuery();
                    
                    while(rs4.next()){
                        System.out.println("Total Grandmasters in League: "+rs4.getString(1));
                    }
                    startCmd();
                    break;
                case 6:
                    CallableStatement cstmt_bestClubRating = conn.prepareCall("{call bestClubRating(?)}");
                    System.out.println("Please Enter Search by Club Name: ");
                    cstmt_bestClubRating.setString("club", sc.nextLine());
                    ResultSet rs5 = cstmt_bestClubRating.executeQuery();
                    
                    while(rs5.next()){
                        String out5 = ("Player Name: "+rs5.getString(1)+"           FIDE Ranking: "+rs5.getString(3)+"          FIDE Title: "+rs5.getString(4));
                        System.out.println(out5);
                               
                    }
                    startCmd();
                break;
                    
                case 7:
                    CallableStatement cstmt_avgClubFide = conn.prepareCall("{call getClubsAvgFide()}");
                    ResultSet rs6 = cstmt_avgClubFide.executeQuery();
                    while(rs6.next()){
                        System.out.println("Club Name: "+rs6.getString(1)+"         Average FIDE Ranking: "+rs6.getString(2));
                    }    
                    startCmd();
                break;
                
                case 8:
                    CallableStatement cstmt_clubTotalPlayersOver = conn.prepareCall("{call getClubTotalPlayersOver(?)}");
                    System.out.println("Enter Minimum Club Members: ");
                    cstmt_clubTotalPlayersOver.setInt("desired_players", sc.nextInt());
                    ResultSet rs7 = cstmt_clubTotalPlayersOver.executeQuery();
                    while(rs7.next()){
                        System.out.println("Club Name: "+rs7.getString(1)+"          Total Players: "+rs7.getString(2));
                    }
                    startCmd();
                break;
                   
            }
            
                

        } catch (Exception exc) {
            System.out.println(exc.toString());
        }

    }

}
