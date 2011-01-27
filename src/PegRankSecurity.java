import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;
import java.text.SimpleDateFormat;
import java.util.logging.*;
import java.sql.*;
import java.io.*;

public class PegRankSecurity extends Plugin {
  //General
  private PegRankSecurity.Listener l = new PegRankSecurity.Listener(this);
  protected static final Logger log = Logger.getLogger("Minecraft");
  public static String name = "PegRankSecurity";
  public static String version = "1.0";
  public static String propFile = "PegRankSecurity.properties";
  public static PropertiesFile props;
  
  public static String connectorJar = "mysql-connector-java-bin.jar";
  public static String pluginColor = Colors.Red; //"\u00a74"
  public static String pluginAltColor = Colors.Gold; //"\u00a74"
  
  //Interaction tracking
  private Block lastface = null;
  
  //Eats new blocks and writes to db
  private Consumer consumer = null;
  
  //Block cue
  private LinkedBlockingQueue<BlockRow> bqueue = new LinkedBlockingQueue<BlockRow>();

  //Write to db delay
  private static int delay = 10;
  
  //Other Settings
  public static boolean debug = false;
  public static boolean serveroutput = true;
  
  //Own Logger (for blocks)
  private static Logger prslog = null;
  
  private static String ranks = "admin,mod,vip,architect,builder";
  private static String[] ranksarray = null;
  
  //MySQL Settings
  public static String driver = "com.mysql.jdbc.Driver";
  public static String user = "root";
  public static String pass = "root";
  public static String db = "jdbc:mysql://localhost:3306/minecraft";
  public static String blockstable = "blocks";
  public static String extratable = "extra";

  /** Get the plugin ready for use */  
  public void enable() {
	if (!initProps()) {
	  log.severe(name + ": Could not initialise " + propFile);
	  disable();
      return;
	}
	try {
		new JDCConnectionDriver(driver, db, user, pass);
	} catch (Exception ex) {
		log.log(Level.SEVERE, name + ": exception while creation database connection pool", ex);
		return;
	}
	if (!checkTables()) {
		log.log(Level.SEVERE, name + ": couldn't create/access the tables?");
		return;
	}
	
	//Start the consumer
	consumer = new Consumer();
	new Thread(consumer).start();
	
	//Is debug on?
	if(debug)
		prslog = Logger.getLogger(name);
	
	//Get the ranks array
	if(!ranksArray()) {
		log.log(Level.SEVERE, name + ": I don't understand the ranks, are they comma seperated?");
	}
	
	log.info(name + " " + version + " Plugin Enabled.");
  }
  
  /** Disable the plugin */
  public void disable() {
	//Stop the consumer
    if (consumer != null) consumer.stop();
    
    //Clear consumer
    consumer = null;
	  
	log.info(name + " " + version + " Plugin Disabled.");	
  }
	
  /** Initialise the plugin (hooks) */
  public void initialize() {
	if(debug) {
		try {
			FileHandler lbfh = new FileHandler(name + ".log", true);
			lbfh.setFormatter(new LogFormatter());
			prslog.addHandler(lbfh);
		} catch (IOException ex) {
			log.info(name + " unable to create logger");
		}
	}

	//etc.getLoader().addListener(PluginLoader.Hook.COMMAND, this.l, this, PluginListener.Priority.LOW);
	etc.getLoader().addListener(PluginLoader.Hook.BLOCK_RIGHTCLICKED, this.l, this, PluginListener.Priority.LOW);
	etc.getLoader().addListener(PluginLoader.Hook.BLOCK_PLACE, this.l, this, PluginListener.Priority.LOW);
	etc.getLoader().addListener(PluginLoader.Hook.BLOCK_BROKEN, this.l, this, PluginListener.Priority.LOW);
	etc.getLoader().addListener(PluginLoader.Hook.SIGN_CHANGE, this.l, this, PluginListener.Priority.LOW);
	//etc.getLoader().addListener(PluginLoader.Hook.ITEM_USE, this.l, this, PluginListener.Priority.LOW);
  }
	
  /** Load the properties file */
  public boolean initProps() {
    props = new PropertiesFile(propFile);
    
    //Rank List
    ranks = props.getString("rank-list", ranks);

    //MySQL
    driver = props.getString("mysql-driver", driver);
    user = props.getString("mysql-user", user);
    pass = props.getString("mysql-pass", pass);
    db = props.getString("mysql-db", db);
    extratable = props.getString("mysql-extratable", extratable);
    blockstable = props.getString("mysql-blockstable", blockstable);
    
    //Delay on write to db
    delay = props.getInt("delay", delay);
    
    //Other
    debug = props.getBoolean("debug-mode", debug);
    serveroutput = props.getBoolean("server-output", serveroutput);
    
    File file = new File(propFile);
    return file.exists();
  }
  
  /* Listener for updates */
  public class Listener extends PluginListener {
	  //////////////////////////////////////////////HERE////////////////////////////////////
	  PegRankSecurity p;
	  
	  public Listener(PegRankSecurity plugin) {
		    this.p = plugin;
	  }
	  
	  public void onBlockRightClicked(Player player, Block blockClicked, Item item){				
			lastface = blockClicked.getFace(blockClicked.getFaceClicked());
			if (debug)
				prslog.info("onBlockRightClicked: clicked " + blockClicked.getType() + " item " + item.getItemId() + " face " + blockClicked.getFace(blockClicked.getFaceClicked()).getType());
	  }  
	  
	  public boolean onBlockPlace(Player player, Block blockPlaced, Block blockClicked, Item itemInHand) {
			if (debug)
			    prslog.info("onBlockPlace: placed " + blockPlaced.getType() + " clicked " + blockClicked.getType() + " item " + itemInHand.getItemId());

			queueBlock(player, lastface, blockPlaced);
			
			return false;
	 }
	  
	 //Someone is breaking something, are they allowed? 
	 public boolean onBlockBreak(Player player, Block b) {
          Connection conn = null;
          PreparedStatement ps = null;
          ResultSet rs = null;
          String msg = null;
          Player owner = null;
          String groupowner = null;
          String groupuser = null;
          int levelowner = 0;
          int leveluser = 0;
          
          try 
          {
              conn = getConnection();
              conn.setAutoCommit(false);
              ps = conn.prepareStatement("SELECT * from blocks left join extra using (id) where y = ? and x = ? and z = ? order by date desc limit 1", Statement.RETURN_GENERATED_KEYS);
              ps.setInt(1, b.getY());
              ps.setInt(2, b.getX());
              ps.setInt(3, b.getZ());
              rs = ps.executeQuery();
              while (rs.next()) {
                  msg = rs.getString("player");
                  owner = etc.getDataSource().getPlayer(msg);
                  try
                  {
                      groupowner = owner.getGroups()[0]; //What group is the person who made the block?
                  }
                  catch (Exception e)
                  {
                      groupowner = null; //None?!?
                  }
              }
              
              try {
                  groupuser = player.getGroups()[0];  //What group is the person who is destroying the block?
              }
              catch(Exception e) {
                  groupuser = null; //None?!?
              }
              
              
              //Set the user group RANK
              leveluser = groupToRank(groupuser);
              
              //Set the owner group RANK
              levelowner = groupToRank(groupowner);
              
              if (leveluser < levelowner) {
                  player.sendMessage(pluginColor + "Your group is too low to remove that block!");
                  player.sendMessage(pluginColor + "It was placed by a " + rankToGroup(levelowner));
                  if(serveroutput) log.info(name + " " + player.getName() + " was denied block placement");
                  return true;
              }
              
          } 
          catch (SQLException ex) {
              log.log(Level.SEVERE, name + " SQL exception", ex); //Error?
          } finally {
              try 
              {
                  if (rs != null)
                      rs.close();
                  if (ps != null)
                      ps.close();
                  if (conn != null)
                      conn.close();
              } 
              catch (SQLException ex) 
              {
                  log.log(Level.SEVERE, name + " SQL exception on close", ex); //Error?
              }
          }
          
          //If they haven't been stopped queue as normal and accept
          queueBlock(player, b, null);
          return false;
      }
  }
  
  //Convert a rank no to a group
  private String rankToGroup(int id) {
	  if(ranksarray[ranksarray.length - id]!=null) {
	   return ranksarray[ranksarray.length - id];
	  } else {
	   return "Unknown";
	  }
  }
  
  //Convert a group (string) to a rank
  private int groupToRank(String group) {
	  if(group!=null) {
		  for(int i = ranksarray.length; i > 0; i--) { //Goes top > bottom
			  if(group.equals(ranksarray[ranksarray.length - i])) return i; //Compares bottom to top
		  }
		  return 0;
	  } else {
		  return 0;
	  }
  }
  
  //Get the ranks
  private boolean ranksArray() {
	  try {
		  ranksarray = ranks.split(",");
	  }
	  catch (Exception e) {
		  return false;
	  }
	  if(ranksarray!=null) {
		  return true;
	  } else { return false; }
  }
  
  
  /* Make logs look like logs... */
  private class LogFormatter extends Formatter {
	public String format(LogRecord rec) {
		return formatMessage(rec) + "\n";
	}
  }
  
  /* Add a block to the queue (for consumer) */
	private void queueBlock(Player player, Block before, Block after) {
		Block b = null;
		int typeA = 0;
		int typeB = 0;
		if (after != null)
		{
			typeA = after.getType();
			b = after;
		}
		if (before != null)
		{
			typeB = before.getType();
			b = before;
		}
	
		if (b == null || typeA < 0 || typeB < 0)
			return;
			
		BlockRow row = new BlockRow(player.getName(), typeB, typeA, b.getX(), b.getY(), b.getZ());
		boolean result = bqueue.offer(row);
		if (debug)
			prslog.info(row.toString());
		if (!result)
			if(serveroutput) log.info(name + " failed to queue block for " + player.getName());
	}

  /* Add a sign to the queue (for consumer) */
	private void queueSign(Player player, Sign sign)
	{
		int type = etc.getDataSource().getItem("sign");
		BlockRow row = new BlockRow(player.getName(), 0, type, sign.getX(), sign.getY(), sign.getZ());
	
		String text = "sign";
		for (int i=0; i < 4; i++)
			text = text + " [" + sign.getText(i) + "]";
		row.addExtra(text);
	
		boolean result = bqueue.offer(row);
		if (debug)
			prslog.info(row.toString());
		if (!result)
			if(serveroutput) log.info(name + " failed to queue block for " + player.getName());
	}
	
	private int parseTimeSpec(String ts)
	{
		String[] split = ts.split(" ");
		
		if (split.length < 2)
			return 0;
			
		int min;
		try {
			min = Integer.parseInt(split[0]);
		} catch (NumberFormatException ex) {
			return 0;
		}
		
		if (split[1].startsWith("hour"))
			min *= 60;
		else if (split[1].startsWith("day"))
			min *= (60*24);
		
		return min;
	}
	
  ///// CONSUMER LOOP ////////
	private class Consumer implements Runnable // start
	{
		private boolean stop = false;
		Consumer() { stop = false; }
		public void stop() { stop = true; }
		public void run()
		{
			PreparedStatement ps = null;
			Connection conn = null;
			BlockRow b;
			
			while (!stop) {
			   long start = System.currentTimeMillis()/1000L;
				int count = 0;
				
				if (bqueue.size() > 100)
					if(serveroutput) log.info(name + " queue size " + bqueue.size());
								
				try {
					conn = getConnection();
					conn.setAutoCommit(false);
					while (count < 100 && start+delay > (System.currentTimeMillis()/1000L)) {
						
						b = bqueue.poll(1L, TimeUnit.SECONDS);

						if (b == null)
							continue;
						
						ps = conn.prepareStatement("INSERT INTO blocks (date, player, replaced, type, x, y, z) VALUES (now(),?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
						ps.setString(1, b.name);
						ps.setInt(2, b.replaced);
						ps.setInt(3, b.type);
						ps.setInt(4, b.x);
						ps.setInt(5, b.y);
						ps.setInt(6, b.z);
						ps.executeUpdate();
						
						if (b.extra != null)
						{
							ResultSet keys = ps.getGeneratedKeys();
							keys.next();
							int key = keys.getInt(1);
							
							ps = conn.prepareStatement("INSERT INTO extra (id, extra) values (?,?)");
							ps.setInt(1, key);
							ps.setString(2, b.extra);
							ps.executeUpdate();
						}
						
						count++;
					}
					if (debug && count > 0)
						prslog.info("Commiting " + count + " inserts.");
					conn.commit();
				} catch (InterruptedException ex) {
					log.log(Level.SEVERE, name + " interrupted exception", ex);
				} catch (SQLException ex) {
					log.log(Level.SEVERE, name + " SQL exception", ex);
				} finally {
					try {
						if (ps != null)
							ps.close();
						if (conn != null)
							conn.close();
					} catch (SQLException ex) {
						log.log(Level.SEVERE, name + " SQL exception on close", ex);
					}
				}
			}
		}
	} // end LBDB
	
  ///// Row of blocks to add to db /////
	private class BlockRow // start
	{
		public String name;
		public int replaced, type;
		public int x, y, z;
		public String extra;
		
		BlockRow(String name, int replaced, int type, int x, int y, int z)
		{
			this.name = name;
			this.replaced = replaced;
			this.type = type;
			this.x = x;
			this.y = y;
			this.z = z;
			this.extra = null;
		}

		public void addExtra(String extra)
		{
			this.extra = extra;
		}
		
		public String toString()
		{
			return("name: " + name + " before type: " + replaced + " type: " + type + " x: " + x + " y: " + y + " z: " + z);
		}
	} // end BlockRow
  
  ///// SQLMethods ////////
  private static String sqlMakeBlocksTable = "CREATE TABLE IF NOT EXISTS  `" + blockstable + "` (" +
  	"`id` int(11) NOT NULL AUTO_INCREMENT," +
  	"`date` datetime NOT NULL DEFAULT '0000-00-00 00:00:00'," +
	"`player` varchar(32) NOT NULL DEFAULT '-'," +
	"`replaced` int(11) NOT NULL DEFAULT '0'," +
	"`type` int(11) NOT NULL DEFAULT '0'," +
	"`x` int(11) NOT NULL DEFAULT '0'," +
	"`y` int(11) NOT NULL DEFAULT '0'," +
	"`z` int(11) NOT NULL DEFAULT '0'," +
	"PRIMARY KEY (`id`)," +
	"KEY `coords` (`y`,`x`,`z`)," +
	"KEY `type` (`type`)," +
	"KEY `replaced` (`replaced`)," +
	"KEY `player` (`player`)" +
  ") ENGINE=MyISAM DEFAULT CHARSET=latin1";
  
  private static String sqlMakeExtraTable = "CREATE TABLE IF NOT EXISTS  `" + extratable + "` (" +
  	"`id` int(11) NOT NULL," +
  	"`extra` text," +
  	"PRIMARY KEY (`id`) " +
  ") ENGINE=MyISAM DEFAULT CHARSET=latin1";
  private static String sqlExtraTableExist = "SHOW TABLES LIKE '" + extratable + "'";
  private static String sqlBlocksTableExist = "SHOW TABLES LIKE '" + blockstable + "'";
  
  /* Get the current connection */
  private Connection getConnection() throws SQLException {
	return DriverManager.getConnection("jdbc:jdc:jdcpool");
  }
  
  /* Check the tables are ready & created */
  private boolean checkTables() {
	Connection conn = null;
	ResultSet rs = null;
	Statement s = null;
	try {
		conn = getConnection();
		
		s = conn.createStatement();
        s.executeUpdate(sqlMakeBlocksTable);
        s.executeUpdate(sqlMakeExtraTable);
		
        rs = s.executeQuery(sqlBlocksTableExist);
        if (rs.first()) {
        	rs = s.executeQuery(sqlExtraTableExist);
        	if (rs.first()) return true;
        }
        return false;
	} catch (SQLException ex) {
		log.log(Level.SEVERE, name + " SQL exception", ex);
	} finally {
		try {
			if (rs != null)
				rs.close();
			if (conn != null)
				conn.close();
		} catch (SQLException ex) {
			log.log(Level.SEVERE, name + " SQL exception on close", ex);
		}
	}
	return false;
  }
}