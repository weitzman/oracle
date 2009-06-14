<?php
// $Id$

/**
 * @file
 * Database interface code for Oracle database servers.
 */


define('ORACLE_EMPTY_STRING_REPLACER','^'); // used to replace '' character in queries
define('ORACLE_IDENTIFIER_MAX_LENGTH',30); // maximum oracle identifier length (e.g. table names cannot exceed this length)  
define('ORACLE_LONG_IDENTIFIER_PREFIX','L#'); // prefix used for long identifier keys
define('ORACLE_BLOB_PREFIX','B^#'); // prefix used for BLOB values
define('ORACLE_MAX_VARCHAR2_LENGTH',4000); // maximum length for a string value in a table column in oracle 
define('ORACLE_ROWNUM_ALIAS','RWN_TO_REMOVE'); // alias used for queryRange filtering (we have to remove that from resultsets)

openlog("oracle", LOG_PID | LOG_PERROR, LOG_LOCAL0);

/**
 * @ingroup database
 * @{
 */

include_once DRUPAL_ROOT . '/includes/database/prefetch.inc';

class DatabaseConnection_oracle extends DatabaseConnection
{
	
  // long identifier handler class
  public $lih;
  
  private $oraclePrefix= array();
	
  // shortcut function to execute statements directly on the database without checks
  public function oracleQuery($query, $args=NULL)
  {
  	  $stmt= parent::prepare($query);
  	  
  	  try
  	  {
  	    $stmt->execute($args);
  	  }
  	  catch (Exception $e)
  	  {
  	  	 syslog(LOG_ERR,"error: ".$e->getMessage()." ".$query);
  	  	 throw $e;
  	  }

  	  return $stmt;
  }
  
  public function __construct(array $connection_options = array())
  {
    //syslog(LOG_ERR,"ORACLE CONNECTION");
    // We don't need a specific PDOStatement class here, we simulate it below.
    $this->statementClass = NULL;
    
  	// This driver defaults to transaction support, except if explicitly passed FALSE.
    $this->transactionSupport = !isset($connection_options['transactions']) || ($connection_options['transactions'] !== FALSE);

    // Transactional DDL is not available in Oracle,
    $this->transactionalDDLSupport = FALSE;
    
    // Default to TCP connection on port 5432.
    if (empty($connection_options['port'])) 
      $connection_options['port'] = 1521;
    

    $dsn = 'oci:dbname=//' . $connection_options['host'] . ':' . $connection_options['port'].'/' . $connection_options['database'] . ';charset=AL32UTF8';
    parent::__construct($dsn, $connection_options['username'], $connection_options['password'], array(

      // Convert numeric values to strings when fetching.
      PDO::ATTR_STRINGIFY_FETCHES => TRUE,
      
      // Force column names to lower case.
      PDO::ATTR_CASE => PDO::CASE_LOWER
      
    ));
    
    // FIXME: already done by DatabaseConnection but anyway seems not to be hold
    $this->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    $this->setAttribute(PDO::ATTR_CASE, PDO::CASE_LOWER);
    $this->setAttribute(PDO::ATTR_STRINGIFY_FETCHES, TRUE);
    
    $options= array('return' => Database::RETURN_NULL);
    
    // force cursor sharing to prevent unfair modules (read more on cursor_sharing http://www.oracle.com/technology/oramag/oracle/06-jan/o16asktom.html) 
    $this->oracleQuery("ALTER SESSION SET cursor_sharing='FORCE'");
    
    // force decimal characters used in string representation of floats to avoid casting problems
    $this->oracleQuery("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'");
    
    // initialize the long identifier handler
    $this->lih= new DatabaseLongIdentifierHandler_oracle($this);
    
    // initialize db_prefix cache
    $this->oraclePrefix= array();
  }
  
  public function query($query, array $args = array(), $options = array(), $retried = FALSE) {

	// Use default values if not already set.
    $options += $this->defaultOptions();

    try {
      if ($query instanceof PDOStatement) {
        $stmt = $query;
        $stmt->execute(NULL);
      }
      else {
        //syslog(LOG_ERR,"query: ". $query." args: ".print_r($args,true)." ret ".$options['return']);
      	$modified = $this->expandArguments($query, $args);
        $stmt = $this->prepareQuery($query);
        
        $stmt->execute($this->cleanupArgs($args), $options);
      }
      
      //syslog(LOG_ERR,"options: ".print_r($options,true));

      switch ($options['return']) {
        case Database::RETURN_STATEMENT:
          return $stmt;
        case Database::RETURN_AFFECTED:
          return $stmt->rowCount();
        case Database::RETURN_INSERT_ID:
          //throw new Exception("INSERT_ID: ".$options['sequence_name']);
          return (isset($options['sequence_name']) ? $this->lastInsertId($options['sequence_name']) : false);
        case Database::RETURN_NULL:
          return;
        default:
          throw new PDOException('Invalid return directive: ' . $options['return']);
      }
    }
    catch (PDOException $e) {
      
      if (isset($e->errorInfo)&&is_array($e->errorInfo)&&$e->errorInfo[1]=='00972'&&!$retried) // catch long identifier errors for alias columns
      {
          $this->lih->findAndRemoveLongIdentifiers($query);
          return $this->query($query, $args, $options, TRUE);
      }
         
      try { $this->rollBack(); } catch (Exception $ex) {}
      
      _db_check_install_needed();
      if ($options['throw_exception']) {
        if ($query instanceof PDOStatement) {
          $query_string = $stmt->queryString;
        }
        elseif (isset($stmt)) {
          $query_string = $stmt->getQueryString();
        }
        else {
          $query_string = $query; 	
        }
        
        syslog(LOG_ERR,"error query: ". $query_string ." e: ".$e->getMessage()." args: ".print_r($args,true));
        throw new PDOException($query_string . " - \n" . print_r($args,1) ." e: ".$e->getMessage());
      }
      
      return NULL;
    }
  }
  
  public function lastInsertId($sequence_name) {
  	return $this->oracleQuery($this->prefixTables("select ".$sequence_name.".currval from dual"))->fetchColumn();
  }

  public function queryRange($query, array $args, $from, $count, array $options = array()) {
  	
    $start= ((int)$from+1);
    $end= ((int)$count+(int)$from);
    
    $queryRange= 'SELECT * FROM (SELECT TAB.*, ROWNUM '.ORACLE_ROWNUM_ALIAS.' FROM ('. $query .') TAB) WHERE '.ORACLE_ROWNUM_ALIAS.' BETWEEN ';
    
    if (DatabaseConnection_oracle::is_assoc($args))
    {
    	 $args["oracle_rwn_start"]= $start;
    	 $args["oracle_rwn_end"]= $end;
         $queryRange.= ':oracle_rwn_start AND :oracle_rwn_end';
    }
    else  	
    {
    	 $args[]= $start;
    	 $args[]= $end;
         $queryRange.= '? AND ?';
    }
    
    return $this->query( $queryRange, $args, $options );

  }

  public function queryTemporary($query, array $args, array $options = array()) {
    $tablename = $this->generateTemporaryTableName();
    try { db_query("DROP TABLE {". $tablename ."}"); } catch (Exception $ex) { /* ignore drop errors */ }
    db_query('CREATE GLOBAL TEMPORARY TABLE {'. $tablename .'} ON COMMIT PRESERVE ROWS AS '.$query, $args);
    return $tablename;
  }
  
  public function generateTemporaryTableName()
  {
  	  // FIXME: create a cleanup job
  	  return "TMP_".$this->oracleQuery("SELECT sid from v\$session where audsid = userenv('sessionid')")->fetchColumn()."_".$this->temporaryNameIndex++; 
  }
  
  public function quote($string)
  {
  	  return "'".str_replace("'","''",$string)."'";
  }

  public function driver() {
    return 'oracle';
  }

  public function databaseType() {
    return 'oracle';
  }

  public function mapConditionOperator($operator) {
    // We don't want to override any of the defaults.
    return NULL;
  }

  /**
   * @todo Remove this as soon as db_rewrite_sql() has been exterminated.
   */
  public function distinctField($table, $field, $query) {
    $field_to_select = 'DISTINCT(' . $table . '.' . $field . ')';
    // (?<!text) is a negative look-behind (no need to rewrite queries that already use DISTINCT).
    return preg_replace('/(SELECT.*)(?:' . $table . '\.|\s)(?<!DISTINCT\()(?<!DISTINCT\(' . $table . '\.)' . $field . '(.*FROM )/AUsi', '\1 ' . $field_to_select . '\2', $query);
  }
  
  public function checkDbPrefix($db_prefix)
  {
	    if (empty($db_prefix)) return;
	
	    if (!isset($this->oraclePrefix[$db_prefix]))
	    {
	    	$this->oracleQuery("begin identifier.check_db_prefix(?); end;",array($db_prefix));
	    	$this->oraclePrefix[$db_prefix]= 'x';
	    }
  }
  
  public function prefixTables($sql) {
	  global $db_prefix;
	
      if (empty($db_prefix))
	    return strtr($sql, array('{' => '', '}' => ''));
	  elseif (is_array($db_prefix))
	  {
		    if (array_key_exists('default', $db_prefix))
		    {
		      $tmp = $db_prefix;
		      unset($tmp['default']);
		      
		      foreach ($tmp as $key => $val)
		      {
		      	 $this->checkDbPrefix($val);
		         $sql = strtr($sql, array('{'. strtoupper($key) .'}' => (empty($val) ? strtoupper($key) : strtoupper($val) .'"."'. strtoupper($key))));
		      }
		      
		      $this->checkDbPrefix($db_prefix['default']);
		      return strtr($sql, array('{' => (empty($db_prefix['default']) ? '' : strtoupper($db_prefix['default']). '"."'), '}' => ''));
		    }
		    else
		    {
		      foreach ($db_prefix as $key => $val)
		      { 
		      	 $this->checkDbPrefix($val);
		         $sql = strtr($sql, array('{'. strtoupper($key) .'}' => (empty($val) ? strtoupper($key) : strtoupper($val) .'"."'. strtoupper($key))));
		      }
		      return strtr($sql, array('{' => '', '}' => ''));
		    }
	  }
	  else
	  {
	  	 $this->checkDbPrefix($db_prefix);
	     return strtr($sql, array('{' =>  strtoupper($db_prefix) . '"."', '}' => ''));
	  }	  	  
  }
  
  // TODO: would be wonderfull to enble a memcached switch here
  protected function prepareQuery($query) {
    $iquery = md5($this->prefixTables($query));
    
    if (empty($this->preparedStatements[$iquery])) {
      $oquery= $query;
      
      $oquery= $this->escapeEmptyLiterals($oquery);
      $oquery= $this->escapeAnsi($oquery);
      $oquery= $this->lih->escapeLongIdentifiers($oquery);
      $oquery= $this->escapeReserved($oquery);
      $oquery= $this->escapeCompatibility($oquery);
      $oquery= $this->prefixTables($oquery);
      
      //syslog(LOG_ERR,"oquery: ".$oquery);
      $this->preparedStatements[$iquery] = $this->prepare($oquery);
    }
    
    return $this->preparedStatements[$iquery];
  }

  public function prepare($query, $options = array()) {
    return new DatabaseStatement_oracle($this, $query, $options);
  }
  
  public function PDOPrepare($query, array $options = array()) {
    return parent::prepare($query, $options);
  }
  
  private function escapeAnsi($query)
  {
  	 if (preg_match("/^select /i",$query)&&!preg_match("/^select(.*)from/ims",$query))
  	  $query.= ' FROM DUAL';

     $search = array ('/("\w+?")/e');

     $replace = array ("strtoupper('\\1')");

     return str_replace('\\"','"',preg_replace($search, $replace, $query));  	
  	  
  }
  
  private function escapeEmptyLiteral($match)
  {
	 if ($match[0]=="''") 
	   return "'".ORACLE_EMPTY_STRING_REPLACER."'";
	 else
	   return $match[0];
  }

  private function escapeEmptyLiterals($query)
  {
     return preg_replace_callback("/'.*?'/", array($this,'escapeEmptyLiteral'), $query);
  }
    
  private function escapeReserved($query)
  {
        $search = array ("/({)(\w+)(})/e", // escapes all table names
                         "/({L#)([0-9]+)(})/e", // escapes long id
		                 "/([\:])(uid|session|file|access|mode|comment)/e",
			   	         "/(<uid>|<session>|<file>|<access>|<mode>|<comment>)/e",
			   	         "/([\(\.\s,])(uid|session|file|access|mode|comment)([,\s\=)])/e");

	    $replace = array ("'\"\\1'.strtoupper('\\2').'\\3\"'",
				          "'\"\\1'.strtoupper('\\2').'\\3\"'",
				          "'\\1'.'db_'.'\\2'.'\\3'",
					      "strtoupper('\"\\1\"')",
					      "'\\1'.strtoupper('\"\\2\"').'\\3'");

        return preg_replace($search, $replace, $query);  	
  }
  
  
  public function removeFromCachedStatements($query)
  {
  	   $iquery = md5($this->prefixTables($query));
       if (isset($this->preparedStatements[$iquery])) 
         unset($this->preparedStatements[$iquery]);
  }
  
  private function escapeCompatibility($query)
  {
		$search = array ("''||", // remove empty concatenations leaved by concatenate_bind_variables
	                     "||''",
	                     "IN ()", // translate 'IN ()' to '= NULL' they do not match anything anyway (always false)
	                     '(FALSE)',
	                     'POW(',
	                     ") AS count_alias", // ugly hacks here
	                     'ON (nid) * FROM "{NODE}"',
		                 'status &');
	
		$replace = array ("",
	                      "",
	                      "= NULL",
	                      "(1=0)",
	                      "POWER(",
	                      ") count_alias",// ugly hacks replace strings here
	                      'x.* FROM "{NODE}" x',
		                  'status= 1 and');
	  	
		return str_replace($search, $replace, $query);
  }
  
  public function makeSequenceName($table, $field) {
    return $this->schema()->makeSequenceName($table, $field);
  }
    
  public function cleanupArgValue($value)
  {
  	    if (is_string($value))
  	    {
  	      if ($value=='')
  	        return ORACLE_EMPTY_STRING_REPLACER;
  	      elseif (strlen($value)>ORACLE_MAX_VARCHAR2_LENGTH)
  	        return $this->writeBlob($value);
  	      else
  	        return $value;
  	    }
  	    else
  	      return $value;
  }
  
  public function cleanupArgs($args)
  {
  	  $ret= array();
  	  
  	  if (DatabaseConnection_oracle::is_assoc($args))
  	    foreach ($args as $key => $value)
  	    {
  	    	 $key= DatabaseConnection_oracle::escapeReserved($key); // bind variables cannot have reserved names
  	    	 $ret[$key]= $this->cleanupArgValue($value);
  	    }
  	  else
   	    foreach ($args as $key => $value)
  	  	  $ret[$key]= $this->cleanupArgValue($value);
  	      
  	  return $ret;
  }
  
  public function writeBlob($value)
  {
  	   $hash= md5($value);
  	   $handle= 0;
  	   
  	   $stream= DatabaseConnection_oracle::stringToStream($value);
  	   
  	   $stmt= parent::prepare("begin identifier.write_blob(?,?,?); end;");
  	   $stmt->bindParam(1, $hash, PDO::PARAM_STR, 32);
       $stmt->bindParam(2, $handle, PDO::PARAM_INT|PDO::PARAM_INPUT_OUTPUT, 32);
       $stmt->bindParam(3, $stream, PDO::PARAM_LOB);

       $stmt->execute();
       
       $handle= ORACLE_BLOB_PREFIX.$handle;
       
       //syslog(LOG_ERR,"returning handle: ".$handle);
       
       return $handle;
  }
  
  public function readBlob($handle)
  {
  	  $handle= (int)substr($handle,strlen(ORACLE_BLOB_PREFIX));
  	  $stmt= parent::prepare("select content from blobs where blobid= ?");
      $stmt->bindParam(1, $handle, PDO::PARAM_INT, 32);
      $stmt->execute();
 	  return $stmt->fetchObject()->content;
  }
  
  public function cleanupFetchedValue($value)
  {
  	  if (is_string($value))
  	  {
  	    if ($value==ORACLE_EMPTY_STRING_REPLACER)
  	      return '';
  	    elseif ($this->isBlob($value))
  	      return $this->readBlob($value);
  	    else
  	      return $value;
  	  }
  	  else
  	    return $value;
  }
  
  public function resetLongIdentifiers()
  {
  	 $this->lih->resetLongIdentifiers();
  }
  
  public static function isLongIdentifier($key)
  {
  	 return (substr(strtoupper($key),0,strlen(ORACLE_LONG_IDENTIFIER_PREFIX))==ORACLE_LONG_IDENTIFIER_PREFIX);
  }
  
  public static function isBlob($value)
  {
  	 return (substr($value,0,strlen(ORACLE_BLOB_PREFIX))==ORACLE_BLOB_PREFIX);
  }
  
  private static function stringToStream($value)
  {
     $stream = fopen('php://memory', 'a');
     fwrite($stream, $value);
     rewind($stream);
  	 return $stream;
  }
  
  public static function is_assoc($array) 
  {
     return (is_array($array) && 0 !== count(array_diff_key($array, array_keys(array_keys($array)))));
  }
  
}


class DatabaseStatement_oracle extends DatabaseStatementPrefetch implements Iterator, DatabaseStatementInterface {
  
  public function __construct(DatabaseConnection $connection, $query, array $driver_options = array()) {
    $this->dbh = $connection;
    $this->queryString = $query;
    $this->driverOptions = $driver_options;
  }
  
  public function execute($args = array(), $options = array()) {
    if (isset($options['fetch'])) {
      if (is_string($options['fetch'])) {
        // Default to an object. Note: db fields will be added to the object
        // before the constructor is run. If you need to assign fields after
        // the constructor is run, see http://drupal.org/node/315092.
        $this->setFetchMode(PDO::FETCH_CLASS, $options['fetch']);
      }
      else {
        $this->setFetchMode($options['fetch']);
      }
    }
    $this->dbh->lastStatement = $this;

    $logger = $this->dbh->getLogger();
    if (!empty($logger)) {
      $query_start = microtime(TRUE);
    }

    // Prepare the query.
    $statement = $this->getStatement($this->queryString, $args);
    if (!$statement) {
      $this->throwPDOException();
    }

    $return = $statement->execute($args);
    if (!$return) {
      $this->throwPDOException();
    }

    // Fetch all the data from the reply, in order to release any lock
    // as soon as possible.
    $this->rowCount = $statement->rowCount();
    
    try
    {
        $this->data = $statement->fetchAll(PDO::FETCH_ASSOC);
    }
    catch (Exception $e)
    {
    	if (!(isset($e->errorInfo)&&is_array($e->errorInfo)&&$e->errorInfo[1]=='24374')) //ignore non-fetchable statements errors 
          throw $e;
    }
    
    unset($statement);

    $this->resultRowCount = count($this->data);

    if ($this->resultRowCount) {
      $this->columnNames = array_keys($this->data[0]);
    }
    else {
      $this->columnNames = array();
    }

    if (!empty($logger)) {
      $query_end = microtime(TRUE);
      $logger->log($this, $args, $query_end - $query_start);
    }

    // Initialize the first row in $this->currentRow.
    $this->next();

    return $return;
  }
  
  /**
   * 
   * @param $f fetched object
   * @return $f cleaned up from:
   * 
   *  1) long identifiers place holders (may occur in queries like:
   *               select 1 as myverylongidentifier from mytable 
   *     this is transalted on query submission as e.g.: 
   *               select 1 as L#321 from mytable
   *     so when we fetch this object (or array) we will have
   *     stdClass ( "L#321" => 1 ) or Array ( "L#321" => 1 ).
   *     but the code is especting to access the field as myobj->myverylongidentifier,
   *     so we need to translate the "L#321" back to "myverylongidentifier").
   *     
   *  2) blob placeholders:
   *     we can find values like B^#2354, and we have to translate those values
   *     back to their original long value so we read blob id 2354 of table blobs   
   *     
   *  3) removes the rwn column from queryRange queries
   *  
   *  4) translate empty string replacement back to empty string
   *  
   */
  private function cleanupFetched($f)
  {
  	    if (is_array($f))
  	      foreach ($f as $key => $value)
  	      {
  	      	if ((string)$key==strtolower(ORACLE_ROWNUM_ALIAS))
  	      		unset($f[$key]);
  	      	elseif (DatabaseConnection_oracle::isLongIdentifier($key)) // long identifier
  	      	{
  	      		$f[$this->dbh->lih->longIdentifierKey($key)]= $this->cleanupFetched($value);
  	      		unset($f[$key]);
  	      	}
  	      	else
  	          $f[$key]= $this->cleanupFetched($value);
  	      }
  	    elseif (is_object($f))
          foreach ($f as $key => $value)
  	      {
  	      	if ((string)$key==strtolower(ORACLE_ROWNUM_ALIAS))
  	      		unset($f->{$key});
  	      	elseif (DatabaseConnection_oracle::isLongIdentifier($key)) // long identifier
  	      	{
  	      	  $f->{$this->dbh->lih->longIdentifierKey($key)}= $this->cleanupFetched($value);
  	      	  unset($f->{$key});
  	      	}
  	      	else
  	      	  $f->{$key}= $this->cleanupFetched($value);
  	      }
  	    else
  	        $f= $this->dbh->cleanupFetchedValue($f);
  	        
  	    return $f;
  }

  public function getStatement($query= NULL, &$args = array()) {
  	  //syslog(LOG_ERR,"lallero getStatement");
  	return $this->dbh->PDOPrepare($this->queryString);
  }
  
  public function current() {
  	  //syslog(LOG_ERR,"lallero");
      return $this->cleanupFetched(parent::current());
  } 
  
  public function next() {
  	  //syslog(LOG_ERR,"lallero");
  	  	return $this->cleanupFetched(parent::next());
  }
   
  public function fetch($fetch_style = NULL, $cursor_orientation = PDO::FETCH_ORI_NEXT, $cursor_offset = NULL) {
  	  //syslog(LOG_ERR,"lallero");
  	  	return $this->cleanupFetched(parent::fetch($fetch_style,$cursor_orientation,$cursor_offset));
  }

  public function fetchField($index = 0) {
  	  //syslog(LOG_ERR,"lallero");
  	  	return $this->cleanupFetched(parent::fetchField($index));
  }

  public function fetchObject($class_name = NULL, $constructor_args = array()) {
  	  //syslog(LOG_ERR,"lallero");
  	  	return $this->cleanupFetched(parent::fetchObject($class_name,$constructor_args));
  }

  public function fetchAssoc() {
  	  //syslog(LOG_ERR,"lallero");
  	return $this->cleanupFetched(parent::fetchAssoc());
  }

  public function fetchAll($fetch_style = NULL, $fetch_column = NULL, $constructor_args = NULL) {
  	  //syslog(LOG_ERR,"lallero");
  	return $this->cleanupFetched(parent::fetchAll($fetch_style,$fetch_column,$constructor_args));
  }

  public function fetchCol($index = 0) {
  	  //syslog(LOG_ERR,"lallero");
  	return $this->cleanupFetched(parent::fetchCol($index));
  }

  public function fetchAllKeyed($key_index = 0, $value_index = 1) {
  	  //syslog(LOG_ERR,"lallero");
  	return $this->cleanupFetched(parent::fetchAllKeyed($key_index,$value_index));
  }

  public function fetchAllAssoc($key, $fetch_style = PDO::FETCH_OBJ) {
  	    	  //syslog(LOG_ERR,"lallero");
  	return $this->cleanupFetched(parent::fetchAllAssoc($key,$fetch_style));
  }
  
}

class DatabaseLongIdentifierHandler_oracle
{
  // holds search reg exp pattern to match known long identifiers
  private $searchLongIdentifiers= array();
  
  // holds replacement string to replace known long identifiers
  private $replaceLongIdentifiers= array();
  
  // holds long identifier hashmap
  private $hashLongIdentifiers= array();
  
  // the parent connection
  private $connection;
	
  public function __construct($connection)
  {
    $this->connection = $connection;
    
    // load long identifiers for the first time in this connection
    $this->resetLongIdentifiers();
  }
  
  public function escapeLongIdentifiers($query)
  {
  	    $ret= "";
  	    
  	    // do not replace things in literals
  	    $literals= array(); 
  	    preg_match_all("/'.*?'/", $query,$literals);
  	    
  	    $literals= $literals[0];
  	    
        $replaceable = preg_split("/'.*?'/", $query);
        
        $lidx= 0;

        // assume that a query cannot start with a literal and that 
        foreach ($replaceable as $toescape)
             $ret.= $this->removeLongIdentifiers($toescape).(isset($literals[$lidx]) ? $literals[$lidx++] : "");          
        
        return $ret;  	
  }
  
  public function removeLongIdentifiers($queryPart)
  {
  	  if (count($this->searchLongIdentifiers))
  	    return preg_replace($this->searchLongIdentifiers, $this->replaceLongIdentifiers, $queryPart);
  	  else
  	    return $queryPart;
  }
  
  // TODO: would be wonderfull to enble a memcached switch here
  public function resetLongIdentifiers()
  {
  	
    try
    {
		    $result= $this->connection->oracleQuery("select id, identifier from long_identifiers where substr(identifier,1,3) not in ('IDX','TRG','PK_','UK_')");
		    
		    while ($row= $result->fetchObject())
		    {
		    	  $this->searchLongIdentifiers[]= "/".$row->identifier."/i";
		    	  $this->replaceLongIdentifiers[]= ORACLE_LONG_IDENTIFIER_PREFIX.$row->id;
		    	  $this->hashLongIdentifiers[ORACLE_LONG_IDENTIFIER_PREFIX.$row->id]= strtolower($row->identifier);
		    }
		    
    }
    catch (Exception $ex)
    {
    	 // ignore until long_identifiers table is not created
    }
  }
  
  public function findAndRecordLongIdentifiers($queryPart)
  {
  	preg_match_all("/\w+/", $queryPart, $words);
  	$words = $words[0];
     
  	foreach ($words as $word)
  	  if (strlen($word)>ORACLE_IDENTIFIER_MAX_LENGTH)
  	   $this->connection->schema()->oid($word);
  }
  
  public function findAndRemoveLongIdentifiers($query)
  {
        $this->connection->removeFromCachedStatements($query);

  	    // do not replace things in literals
  	    $literals= array(); 
        $replaceable = preg_split("/'.*?'/", $query);
        
        $lidx= 0;

        // assume that a query cannot start with a literal and that 
        foreach ($replaceable as $toescape)
             $this->findAndRecordLongIdentifiers($toescape);

        $this->resetLongIdentifiers();
  }
  
  public function longIdentifierKey($key)
  {
  	 return $this->hashLongIdentifiers[strtoupper($key)];
  }
   
}

/**
 * @} End of "ingroup database".
 */
