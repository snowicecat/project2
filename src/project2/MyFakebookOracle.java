package project2;

import com.sun.tools.internal.xjc.reader.xmlschema.bindinfo.BIConversion;

import javax.xml.transform.Result;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeSet;
import java.util.Vector;
import java.sql.PreparedStatement;

public class MyFakebookOracle extends FakebookOracle {
	
	static String prefix = "ethanjyx.";
	
	// You must use the following variable as the JDBC connection
	Connection oracleConnection = null;
	
	// You must refer to the following variables for the corresponding tables in your database
	String cityTableName = null;
	String userTableName = null;
	String friendsTableName = null;
	String currentCityTableName = null;
	String hometownCityTableName = null;
	String programTableName = null;
	String educationTableName = null;
	String eventTableName = null;
	String participantTableName = null;
	String albumTableName = null;
	String photoTableName = null;
	String coverPhotoTableName = null;
	String tagTableName = null;
	
	
	// DO NOT modify this constructor
	public MyFakebookOracle(String u, Connection c) {
		super();
		String dataType = u;
		oracleConnection = c;
		// You will use the following tables in your Java code
		cityTableName = prefix+dataType+"_CITIES";
		userTableName = prefix+dataType+"_USERS";
		friendsTableName = prefix+dataType+"_FRIENDS";
		currentCityTableName = prefix+dataType+"_USER_CURRENT_CITY";
		hometownCityTableName = prefix+dataType+"_USER_HOMETOWN_CITY";
		programTableName = prefix+dataType+"_PROGRAMS";
		educationTableName = prefix+dataType+"_EDUCATION";
		eventTableName = prefix+dataType+"_USER_EVENTS";
		albumTableName = prefix+dataType+"_ALBUMS";
		photoTableName = prefix+dataType+"_PHOTOS";
		tagTableName = prefix+dataType+"_TAGS";
	}
	
	
	@Override
	// ***** Query 0 *****
	// This query is given to your for free;
	// You can use it as an example to help you write your own code
	//
	public void findMonthOfBirthInfo() throws SQLException{ 
		ResultSet rst = null; 
		PreparedStatement getMonthCountStmt = null;
		PreparedStatement getNamesMostMonthStmt = null;
		PreparedStatement getNamesLeastMonthStmt = null;
		
		try {
			String getMonthCountSql = "select count(*), month_of_birth from " +
				userTableName +
				" where month_of_birth is not null group by month_of_birth order by 1 desc";
			getMonthCountStmt = oracleConnection.prepareStatement(getMonthCountSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			
			// getMonthCountSql is the query that will run
			// For each month, find the number of friends born that month
			// Sort them in descending order of count
			// executeQuery will run the query and generate the result set
			rst = getMonthCountStmt.executeQuery();
			
			this.monthOfMostFriend = 0;
			this.monthOfLeastFriend = 0;
			this.totalFriendsWithMonthOfBirth = 0;
			while(rst.next()) {
				int count = rst.getInt(1);
				int month = rst.getInt(2);
				if (rst.isFirst())
					this.monthOfMostFriend = month;
				if (rst.isLast())
					this.monthOfLeastFriend = month;
				this.totalFriendsWithMonthOfBirth += count;
			}

			// Get the month with most friends, and the month with least friends.
			// (Notice that this only considers months for which the number of friends is > 0)
			// Also, count how many total friends have listed month of birth (i.e., month_of_birth not null)
			//
			
			// Get the names of friends born in the "most" month
			String getNamesMostMonthSql = "select USER_ID, first_name, last_name from " + 
				userTableName + 
				" where month_of_birth = ?";
			getNamesMostMonthStmt = oracleConnection.prepareStatement(getNamesMostMonthSql);
			
			// set the first ? in the sql above to value this.monthOfMostFriend, with Integer type
			getNamesMostMonthStmt.setInt(1, this.monthOfMostFriend);
			rst = getNamesMostMonthStmt.executeQuery();
			while(rst.next()) {
				Long uid = rst.getLong(1);
				String firstName = rst.getString(2);
				String lastName = rst.getString(3);
				this.friendsInMonthOfMost.add(new UserInfo(uid, firstName, lastName));
			}
			
			// Get the names of friends born in the "least" month
			String getNamesLeastMonthSql = "select first_name, last_name, USER_ID from " + 
				userTableName + 
				" where month_of_birth = ?";
			getNamesLeastMonthStmt = oracleConnection.prepareStatement(getNamesLeastMonthSql);
			getNamesLeastMonthStmt.setInt(1, this.monthOfLeastFriend);
			
			rst = getNamesLeastMonthStmt.executeQuery();
			while(rst.next()){
				String firstName = rst.getString(1);
				String lastName = rst.getString(2);
				Long uid = rst.getLong(3);
				this.friendsInMonthOfLeast.add(new UserInfo(uid, firstName, lastName));
			}
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
			// Close statement and result set
			if(rst != null) 
				rst.close();
			
			if(getMonthCountStmt != null)
				getMonthCountStmt.close();
			
			if(getNamesMostMonthStmt != null)
				getNamesMostMonthStmt.close();
			
			if(getNamesLeastMonthStmt != null)
				getNamesLeastMonthStmt.close();
		}
	}

	
	
	@Override
	// ***** Query 1 *****
	// Find information about friend names:
	// (1) The longest last name (if there is a tie, include all in result)
	// (2) The shortest last name (if there is a tie, include all in result)
	// (3) The most common last name, and the number of times it appears (if there is a tie, include all in result)
	//
	public void findNameInfo() throws SQLException { // Query1
        // Find the following information from your database and store the information as shown
		//this.longestLastNames.add("JohnJacobJingleheimerSchmidt");
		//this.shortestLastNames.add("Ng");
		//this.shortestLastNames.add("Fu");
		//this.shortestLastNames.add("Wu");
		//this.mostCommonLastNames.add("Wang");
		//this.mostCommonLastNames.add("Smith");
		//this.mostCommonLastNamesCount = 10;
		ResultSet rst = null;
		PreparedStatement LLNamesStmt = null;
		PreparedStatement SLNamesStmt = null;
		PreparedStatement MCNamesStmt = null;

		try {
			String LLNamesQ = "SELECT last_name, len FROM (SELECT last_name, length(last_name) len FROM " + userTableName + " GROUP BY last_name) " +
					"WHERE len = (SELECT max(length(last_name)) FROM " + userTableName + " )";
			LLNamesStmt = oracleConnection.prepareStatement(LLNamesQ, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			rst = LLNamesStmt.executeQuery();
			while(rst.next()){
				this.longestLastNames.add(rst.getString(1));
			}

			String SLNamesQ = "SELECT last_name, len FROM (SELECT last_name, length(last_name) len FROM " + userTableName + " GROUP BY last_name) " +
					"WHERE len = (SELECT min(length(last_name)) FROM " + userTableName + " )";
			SLNamesStmt = oracleConnection.prepareStatement(SLNamesQ, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			rst = SLNamesStmt.executeQuery();
			while(rst.next()) {
				this.shortestLastNames.add(rst.getString(1));
			}

			String MCNamesQ = "SELECT last_name, county FROM (SELECT last_name, count(last_name) county FROM " + userTableName +" GROUP BY last_name) " +
					"WHERE county = (SELECT max(count(last_name)) FROM " + userTableName + " ) GROUP BY last_name)";
			MCNamesStmt = oracleConnection.prepareStatement(MCNamesQ, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			rst = MCNamesStmt.executeQuery();
			while(rst.next()) {
				this.mostCommonLastNames.add(rst.getString(1));
				this.mostCommonLastNamesCount = rst.getInt(2);
			}

		} catch(SQLException e) {
			System.err.println(e.getMessage());
			throw e;
		} finally {
			if(rst != null)
				rst.close();

			if(LLNamesStmt != null)
				LLNamesStmt.close();

			if(SLNamesStmt != null)
				SLNamesStmt.close();

			if(MCNamesStmt != null)
				MCNamesStmt.close();
		}



	}
	
	@Override
	// ***** Query 2 *****
	// Find the user(s) who have strictly more than 80 friends in the network
	//
	// Be careful on this query!
	// Remember that if two users are friends, the friends table
	// only contains the pair of user ids once, subject to 
	// the constraint that user1_id < user2_id
	//
	public void popularFriends() throws SQLException {
		// Find the following information from your database and store the information as shown 
		//this.popularFriends.add(new UserInfo(10L, "Billy", "SmellsFunny"));
		//this.popularFriends.add(new UserInfo(11L, "Jenny", "BadBreath"));
		//this.countPopularFriends = 2;
		ResultSet rst = null;
		PreparedStatement MFriendStmt= null;
		try {
			String MFriendQ = "SELECT U.USER_ID, U.first_name, U.last_name FROM " + userTableName + " U, " +
					"(SELECT F1.user1_id, count(F1.user1_id) county FROM " + friendsTableName + " F1, " + friendsTableName +
					" F2 WHERE F1.user1_id = F2.user2_id " +
					"GROUP BY F1.user1_id, F2.user2_id " +
					"HAVING count(F1.user1_id) > 80 ) F " +
					"WHERE U.USER_ID = F.user1_id";

			MFriendStmt = oracleConnection.prepareStatement(MFriendQ, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			rst = MFriendStmt.executeQuery();
			this.countPopularFriends = 0;

			while(rst.next()) {
				this.popularFriends.add(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
				this.countPopularFriends += 1;
			}
		} catch(SQLException e) {
			System.err.println(e.getMessage());
			throw e;
		} finally {
			if(rst != null)
				rst.close();
			if(MFriendStmt != null)
				MFriendStmt.close();
		}
	}
	 

	@Override
	// ***** Query 3 *****
	// Find the users who still live in their hometowns
	// (I.e., current_city = hometown_city)
	//	
	public void liveAtHome() throws SQLException {
		//this.liveAtHome.add(new UserInfo(11L, "Heather", "Hometowngirl"));
		//this.countLiveAtHome = 1;
		ResultSet rst = null;
		PreparedStatement userLiveHomeStmt = null;
		try{
			String userLiveHomeQ = "SELECT U.USER_ID, U.first_name, U.last_name FROM " + userTableName + " U, " + hometownCityTableName + " H, " + currentCityTableName + " C, "
					+ "WHERE U.USER_ID = H.USER_ID AND C.CURRENT_CITY_ID = H.HOMETOWN_CITY_ID AND U.USER_ID = C.USER_ID";
			userLiveHomeStmt = oracleConnection.prepareStatement(userLiveHomeQ, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			rst = userLiveHomeStmt.executeQuery();
			this.countLiveAtHome = 0;

			while(rst.next()) {
				this.liveAtHome.add(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
				this.countLiveAtHome++;
			}
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			throw e;
		} finally {
			if(rst != null)
				rst.close();
			if(userLiveHomeStmt != null)
				userLiveHomeStmt.close();
		}
	}



	@Override
	// **** Query 4 ****
	// Find the top-n photos based on the number of tagged users
	// If there are ties, choose the photo with the smaller numeric PhotoID first
	// 
	public void findPhotosWithMostTags(int n) throws SQLException { 
		//String photoId = "1234567";
		//String albumId = "123456789";
		//String albumName = "album1";
		//String photoCaption = "caption1";
		//String photoLink = "http://google.com";
		//PhotoInfo p = new PhotoInfo(photoId, albumId, albumName, photoCaption, photoLink);
		//TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
		//tp.addTaggedUser(new UserInfo(12345L, "taggedUserFirstName1", "taggedUserLastName1"));
		//tp.addTaggedUser(new UserInfo(12345L, "taggedUserFirstName2", "taggedUserLastName2"));
		//this.photosWithMostTags.add(tp);
		ResultSet rst = null;
		PreparedStatement findAllStmt = null;
		PreparedStatement findPhotoStmt = null;
		PreparedStatement findTagUserStmt = null;
		try {
			String findAllQ = "SELECT * FROM (SELECT tag_photo_id FROM " + tagTableName + " GROUP BY tag_photo_id ORDER BY count(*) desc, tag_photo_id) "
					+ "WHERE rownum <= " + n;
			findAllStmt = oracleConnection.prepareStatement(findAllQ, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			rst = findAllStmt.executeQuery();

			while(rst.next()) {
				String tagPhotoID = rst.getString(1);
				String findPhotoQ = "SELECT P.photo_id, P.album_id, P.album_name, P.photo_caption, P.photo_link FROM "
						+ photoTableName + " P, " + albumTableName + " A " +
						"WHERE P.photo_id = " + tagPhotoID + " AND P.album_id = A.album_id";
				findPhotoStmt = oracleConnection.prepareStatement(findPhotoQ, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				ResultSet rstPhoto = findPhotoStmt.executeQuery();

				rstPhoto.next();
				String photoID = rstPhoto.getString(1);
				String albumID = rstPhoto.getString(2);
				String albumName = rstPhoto.getString(3);
				String photoCaption = rstPhoto.getString(4);
				String photoLink = rstPhoto.getString(5);
				PhotoInfo p = new PhotoInfo(photoID, albumID, albumName, photoCaption, photoLink);
				TaggedPhotoInfo tp = new TaggedPhotoInfo(p);

				String findTagUserQ = "SELECT U.user_id, U.first_name, U.last_name FROM "
						+ userTableName + " U, " + tagTableName + " T, " +
						"WHERE T.tag_subject_id = U.user_id AND T.tag_photo_id = " + tagPhotoID;
				findTagUserStmt = oracleConnection.prepareStatement(findTagUserQ, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				ResultSet rstUser = findTagUserStmt.executeQuery();
				while(rstUser.next()) {
					tp.addTaggedUser(new UserInfo(rstUser.getLong(1), rstUser.getString(2), rstUser.getString(3)));
					this.photosWithMostTags.add(tp);
				}
			}
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			throw e;
		} finally {
			if(rst != null)
				rst.close();
			if(findAllStmt != null)
				findAllStmt.close();
			if(findPhotoStmt != null)
				findPhotoStmt.close();
			if(findTagUserStmt != null)
				findTagUserStmt.close();
		}

	}

	
	
	
	@Override
	// **** Query 5 ****
	// Find suggested "match pairs" of friends, using the following criteria:
	// (1) One of the friends is female, and the other is male
	// (2) Their age difference is within "yearDiff"
	// (3) They are not friends with one another
	// (4) They should be tagged together in at least one photo
	//
	// You should up to n "match pairs"
	// If there are more than n match pairs, you should break ties as follows:
	// (i) First choose the pairs with the largest number of shared photos
	// (ii) If there are still ties, choose the pair with the smaller USER_ID for the female
	// (iii) If there are still ties, choose the pair with the smaller USER_ID for the male
	//
	public void matchMaker(int n, int yearDiff) throws SQLException { 
		Long girlUserId = 123L;
		String girlFirstName = "girlFirstName";
		String girlLastName = "girlLastName";
		int girlYear = 1988;
		Long boyUserId = 456L;
		String boyFirstName = "boyFirstName";
		String boyLastName = "boyLastName";
		int boyYear = 1986;
		MatchPair mp = new MatchPair(girlUserId, girlFirstName, girlLastName, 
				girlYear, boyUserId, boyFirstName, boyLastName, boyYear);
		String sharedPhotoId = "12345678";
		String sharedPhotoAlbumId = "123456789";
		String sharedPhotoAlbumName = "albumName";
		String sharedPhotoCaption = "caption";
		String sharedPhotoLink = "link";
		mp.addSharedPhoto(new PhotoInfo(sharedPhotoId, sharedPhotoAlbumId, 
				sharedPhotoAlbumName, sharedPhotoCaption, sharedPhotoLink));
		this.bestMatches.add(mp);
	}

	
	
	// **** Query 6 ****
	// Suggest friends based on mutual friends
	// 
	// Find the top n pairs of users in the database who share the most
	// friends, but such that the two users are not friends themselves.
	//
	// Your output will consist of a set of pairs (user1_id, user2_id)
	// No pair should appear in the result twice; you should always order the pairs so that
	// user1_id < user2_id
	//
	// If there are ties, you should give priority to the pair with the smaller user1_id.
	// If there are still ties, give priority to the pair with the smaller user2_id.
	//
	@Override
	public void suggestFriendsByMutualFriends(int n) throws SQLException {
		Long user1_id = 123L;
		String user1FirstName = "Friend1FirstName";
		String user1LastName = "Friend1LastName";
		Long user2_id = 456L;
		String user2FirstName = "Friend2FirstName";
		String user2LastName = "Friend2LastName";
		FriendsPair p = new FriendsPair(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);

		p.addSharedFriend(567L, "sharedFriend1FirstName", "sharedFriend1LastName");
		p.addSharedFriend(678L, "sharedFriend2FirstName", "sharedFriend2LastName");
		p.addSharedFriend(789L, "sharedFriend3FirstName", "sharedFriend3LastName");
		this.suggestedFriendsPairs.add(p);
	}
	
	
	//@Override
	// ***** Query 7 *****
	// Given the ID of a user, find information about that
	// user's oldest friend and youngest friend
	// 
	// If two users have exactly the same age, meaning that they were born
	// on the same day, then assume that the one with the larger USER_ID is older
	//
	public void findAgeInfo(Long USER_ID) throws SQLException {
		//this.oldestFriend = new UserInfo(1L, "Oliver", "Oldham");
		//this.youngestFriend = new UserInfo(25L, "Yolanda", "Young");
		ResultSet rst = null;
		PreparedStatement oldestFriendStmt = null;
		PreparedStatement youngestFriendStmt = null;

		try {
			String oldestFriendQ = "SELECT * FROM (SELECT U.user_id, U.first_name, U.last_name FROM " + userTableName + " U, " + friendsTableName + " F "
					+ "WHERE (U.user_id = F.user1_id AND F.user2_id = " + USER_ID + ") " +
					"or (U.user_id = F.user2_id AND F.user1_id = " + USER_ID + ") " +
					"ORDER BY U.year_of_birth ASC, U.month_of_birth ASC, U.day_of_birth ASC, U.user_id DESC) " +
					"WHERE rownum <= 1";
			String youngestFriendQ = "SELECT * FROM (SELECT U.user_id, U.first_name, U.last_name FROM " + userTableName + " U, " + friendsTableName + " F "
					+ "WHERE (U.user_id = F.user1_id AND F.user2_id = " + USER_ID + ") " +
					"or (U.user_id = F.user2_id AND F.user1_id = " + USER_ID + ") " +
					"ORDER BY U.year_of_birth DESC, U.month_of_birth DESC, U.day_of_birth DESC , U.user_id ASC) " +
					"WHERE rownum <= 1";

			oldestFriendStmt = oracleConnection.prepareStatement(oldestFriendQ, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			youngestFriendStmt = oracleConnection.prepareStatement(youngestFriendQ, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

			rst = oldestFriendStmt.executeQuery();
			rst.next();
			this.oldestFriend = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));

			rst = youngestFriendStmt.executeQuery();
			rst.next();
			this.youngestFriend = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));

		} catch (SQLException e) {
			System.err.println(e.getMessage());
			throw e;
		} finally {
			if(rst != null)
				rst.close();
			if(oldestFriendStmt != null)
				oldestFriendStmt.close();
			if(youngestFriendStmt != null)
				youngestFriendStmt.close();
		}
	}
	
	
	@Override
	// ***** Query 8 *****
	// 
	// Find the name of the city with the most events, as well as the number of 
	// events in that city.  If there is a tie, return the names of all of the (tied) cities.
	//
	public void findEventCities() throws SQLException {
		//this.eventCount = 12;
		//this.popularCityNames.add("Ann Arbor");
		//this.popularCityNames.add("Ypsilanti");
		ResultSet rst = null;
		ResultSet crst =null;
		PreparedStatement getCountStmt = null;
		PreparedStatement getCityStmt = null;
		try {
			String getCountQ = "SELECT count(*) as county, event_city_id FROM "
					+ eventTableName + " GROUP BY event_city_id " +
					"ORDER BY county desc";
			getCountStmt = oracleConnection.prepareStatement(getCountQ, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			rst = getCountStmt.executeQuery();
			while(rst.next()) {
				if(rst.isFirst())
					this.eventCount = rst.getInt(1);
				if(rst.getInt(1) == this.eventCount) {
					String city_id = rst.getString(2);
					String getCityQ = "SELECT city_name FROM " + cityTableName + " WHERE city_id = " + city_id;
					getCityStmt = oracleConnection.prepareStatement(getCityQ, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
					crst = getCityStmt.executeQuery();
					crst.next();
					this.popularCityNames.add(crst.getString(1));
				}
				else break;
			}
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			throw e;
		} finally {
			if(rst != null)
				rst.close();
			if(crst != null)
				crst.close();
			if(getCountStmt !=  null)
				getCountStmt.close();
			if(getCityStmt != null)
				getCityStmt.close();
		}
	}
	
	
	
	@Override
//	 ***** Query 9 *****
	//
	// Find pairs of potential siblings and print them out in the following format:
	//   # pairs of siblings
	//   sibling1 lastname(id) and sibling2 lastname(id)
	//   siblingA lastname(id) and siblingB lastname(id)  etc.
	//
	// A pair of users are potential siblings if they have the same last name and hometown, if they are friends, and
	// if they are less than 10 years apart in age.  Pairs of siblings are returned with the lower USER_ID user first
	// on the line.  They are ordered based on the first USER_ID and in the event of a tie, the second USER_ID.
	//  
	//
	public void findPotentialSiblings() throws SQLException {
		//Long user1_id = 123L;
		//String user1FirstName = "Friend1FirstName";
		//String user1LastName = "Friend1LastName";
		//Long user2_id = 456L;
		//String user2FirstName = "Friend2FirstName";
		//String user2LastName = "Friend2LastName";
		//SiblingInfo s = new SiblingInfo(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);
		//this.siblings.add(s);

		ResultSet rst = null;
		PreparedStatement findSibStmt = null;
		try {
			String findSibQ = "SELECT U1.user_id, U1.first_name, U1.last_name, U2.user_id, U2.first_name, U2.last_name " +
					"FROM " + userTableName + " U1, " + userTableName + " U2, " + hometownCityTableName + " H1, " + hometownCityTableName + " H2 " +
					"WHERE U1.last_name = U2.last_name " +
					"AND U1.user_id = H1.user_id AND U2.user_id = H2.user_id " +
					"AND H1.hometown_city_id = H2.hometown_city_id AND U1.user_id < U2.user_id " +
					"AND (U1.user_id, U2.user_id) IN (SELECT user1_id, user2_id FROM " + friendsTableName + " ) " +
					"AND ABS(U1.year_of_birth - U2.year_of_birth) < 10 ORDER BY U1.user_id ASC, U2.user_id ASC";
			findSibStmt = oracleConnection.prepareStatement(findSibQ, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			rst = findSibStmt.executeQuery();

			while(rst.next()) {
				SiblingInfo s = new SiblingInfo(rst.getLong(1), rst.getString(2), rst.getString(3), rst.getLong(4), rst.getString(5), rst.getString(6));
				this.siblings.add(s);
			}

		} catch (SQLException e) {
			System.err.println(e.getMessage());
			throw e;
		} finally {
			if(rst != null)
				rst.close();
			if(findSibStmt != null)
				findSibStmt.close();

		}
	}
	
}