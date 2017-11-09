<?php

namespace Drupal\Driver\Database\oracle;

use Drupal\Core\Database\DatabaseExceptionWrapper;
use Drupal\Core\Database\IntegrityConstraintViolationException;
use Drupal\Core\Database\DatabaseNotFoundException;

use Drupal\Core\Database\Database;
use Drupal\Core\Database\Connection as DatabaseConnection;

/**
 * Used to replace '' character in queries.
 */
define('ORACLE_EMPTY_STRING_REPLACER', '^');

/**
 * Maximum oracle identifier length (e.g. table names cannot exceed this length).
 */
define('ORACLE_IDENTIFIER_MAX_LENGTH', 30);

/**
 * Prefix used for long identifier keys.
 */
define('ORACLE_LONG_IDENTIFIER_PREFIX', 'L#');

/**
 * Prefix used for BLOB values.
 */
define('ORACLE_BLOB_PREFIX', 'B^#');

/**
 * Maximum length for a string value in a table column in oracle.
 * Affects schema.inc table creation.
 */
define('ORACLE_MAX_VARCHAR2_LENGTH', 4000);

/**
 * Maximum length of a string that PDO_OCI can handle.
 * Affects runtime blob creation.
 */
define('ORACLE_MIN_PDO_BIND_LENGTH', 4000);

/**
 * Alias used for queryRange filtering (we have to remove that from resultsets).
 */
define('ORACLE_ROWNUM_ALIAS', 'RWN_TO_REMOVE');

/**
 * @addtogroup database
 */

class Connection extends DatabaseConnection {

  /**
   * Error code for "Unknown database" error.
   */
  const DATABASE_NOT_FOUND = 0;

  /**
   * We are being use to connect to an external oracle database.
   */
  public $external = FALSE;

  private $oraclePrefix = array();

  private $max_varchar2_bind_size = ORACLE_MIN_PDO_BIND_LENGTH;

  protected $statementClass = 'Drupal\Driver\Database\oracle\Statement';
  protected $statementClassOracle = 'Drupal\Driver\Database\oracle\StatementOracle';

  protected $transactionSupport = TRUE;

  public function __construct(\PDO $connection, array $connection_options = array()) {
    global $oracle_user;

    parent::__construct($connection, $connection_options);

    // This driver defaults to transaction support, except if explicitly passed FALSE.
    $this->transactionSupport = !isset($connection_options['transactions']) || ($connection_options['transactions'] !== FALSE);

    // Transactional DDL is not available in Oracle.
    $this->transactionalDDLSupport = FALSE;

    // Needed by DatabaseConnection.getConnectionOptions
    $this->connectionOptions = $connection_options;

    $oracle_user = $connection_options['username'];

    // Setup session attributes.
    try {
      $stmt = parent::prepare("begin ? := setup_session; end;");
      $stmt->bindParam(1, $this->max_varchar2_bind_size, \PDO::PARAM_INT | \PDO::PARAM_INPUT_OUTPUT, 32);

      $stmt->execute();
    }
    catch (\Exception $ex) {
      // Ignore at install time or external databases.
      // Fallback to minimum bind size.
      $this->max_varchar2_bind_size = ORACLE_MIN_PDO_BIND_LENGTH;

      // Connected to an external oracle database (not necessarly a drupal schema).
      $this->external = TRUE;
    }

    // Initialize db_prefix cache.
    $this->oraclePrefix = array();
  }

  /**
   * {@inheritdoc}
   */
  public static function open(array &$connection_options = array()) {
    // Default to TCP connection on port 5432.
    if (empty($connection_options['port'])) {
      $connection_options['port'] = 1521;
    }

    if ($connection_options['host'] == 'USETNS') {
      // Use database as TNSNAME.
      $dsn = 'oci:dbname=' . $connection_options['database'] . ';charset=AL32UTF8';
    }
    else {
      // Use host/port/database.
      $dsn = 'oci:dbname=//' . $connection_options['host'] . ':' . $connection_options['port'] . '/' . $connection_options['database'] . ';charset=AL32UTF8';
    }

    // Allow PDO options to be overridden.
    $connection_options += array(
      'pdo' => array(),
    );

    $connection_options['pdo'] += array(
      \PDO::ATTR_STRINGIFY_FETCHES => TRUE,
      \PDO::ATTR_CASE => \PDO::CASE_LOWER,
      \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
    );

    $pdo = new \PDO($dsn, $connection_options['username'], $connection_options['password'], $connection_options['pdo']);

    return $pdo;
  }

  public function query($query, array $args = array(), $options = array(), $retried = 0) {
    // Use default values if not already set.
    $options += $this->defaultOptions();

    try {
      if ($query instanceof \PDOStatement) {
        $stmt = $query;
        $stmt->execute(empty($args) ? NULL : (array) $args, $options);
      }
      else {
        $this->expandArguments($query, $args);
        $stmt = $this->prepareQuery($query);
        $stmt->execute($this->cleanupArgs($args), $options);
      }

      switch ($options['return']) {
        case Database::RETURN_STATEMENT:
          return $stmt;
        case Database::RETURN_AFFECTED:
          return $stmt->rowCount();
        case Database::RETURN_INSERT_ID:
          return (isset($options['sequence_name']) ? $this->lastInsertId($options['sequence_name']) : FALSE);
        case Database::RETURN_NULL:
          return;
        default:
          throw new \PDOException('Invalid return directive: ' . $options['return']);
      }
    }
    catch (\Exception $e) {
      $query_string = ($query instanceof \PDOStatement) ? $stmt->queryString : $query;

      if ($this->exceptionQuery($query_string) && $retried != 1) {
        return $this->query($query_string, $args, $options, 1);
      }

      // Catch long identifier errors for alias columns.
      if (isset($e->errorInfo) && is_array($e->errorInfo) && $e->errorInfo[1] == '00972' && $retried != 2 && !$this->external) {
        $this->getLongIdentifiersHandler()->findAndRemoveLongIdentifiers($query_string);
        return $this->query($query_string, $args, $options, 2);
      }

      if ($options['throw_exception']) {
        $message = $query_string . (isset($stmt) && $stmt instanceof DatabaseStatementOracle ? " (prepared: ".$stmt->getQueryString() . " )" : "") . " e: " . $e->getMessage() . " args: " . print_r($args, TRUE);
        syslog(LOG_ERR, "error query: " . $message);

        if (strpos($e->getMessage(), 'ORA-00001')) {
          $exception = new IntegrityConstraintViolationException($message, (int) $e->getCode(), $e);
        }
        else {
          $exception = new DatabaseExceptionWrapper($message, (int) $e->getCode(), $e);
        }
        $exception->errorInfo = $e->errorInfo;

        if ($exception->errorInfo[1] == '1') {
          $exception->errorInfo[0] = '23000';
        }
        throw $exception;
      }

      return NULL;
    }
  }

  public function queryRange($query, $from, $count, array $args = array(), array $options = array()) {
    $start = (int) $from + 1;
    $end = (int) $count + (int) $from;

    $query_string = 'SELECT * FROM (SELECT TAB.*, ROWNUM ' . ORACLE_ROWNUM_ALIAS . ' FROM (' . $query . ') TAB) WHERE ' . ORACLE_ROWNUM_ALIAS . ' BETWEEN ';
    if (Connection::isAssoc($args)) {
      $args['oracle_rwn_start'] = $start;
      $args['oracle_rwn_end'] = $end;
      $query_string .= ':oracle_rwn_start AND :oracle_rwn_end';
    }
    else {
      $args[] = $start;
      $args[] = $end;
      $query_string .= '? AND ?';
    }

    return $this->query($query_string, $args, $options);
  }

  public function queryTemporary($query, array $args = array(), array $options = array()) {
    $tablename = $this->generateTemporaryTableName();
    try {
      $this->query("DROP TABLE {". $tablename ."}");
    }
    catch (\Exception $ex) {
      /* ignore drop errors */
    }
    $this->query('CREATE GLOBAL TEMPORARY TABLE {' . $tablename . '} ON COMMIT PRESERVE ROWS AS ' . $query, $args, $options);
    return $tablename;
  }

  public function driver() {
    return 'oracle';
  }

  public function databaseType() {
    return 'oracle';
  }

  /**
   * Overrides \Drupal\Core\Database\Connection::createDatabase().
   *
   * @param string $database
   *   The name of the database to create.
   *
   * @throws DatabaseNotFoundException
   */
  public function createDatabase($database) {
    // Database can be created manualy only.
  }

  public function mapConditionOperator($operator) {
    // We don't want to override any of the defaults.
    return NULL;
  }

  public function nextId($existing_id = 0) {
    // Retrive the name of the sequence. This information cannot be cached
    // because the prefix may change, for example, like it does in simpletests.
    $sequence_name = str_replace('"', '', $this->makeSequenceName('sequences', 'value'));
    $id = $this->query("SELECT " . $sequence_name . ".nextval FROM DUAL")->fetchField();
    if ($id > $existing_id) {
      return $id;
    }

    $id = $this->query("SELECT " . $sequence_name . ".nextval FROM DUAL")->fetchField();
    if ($id > $existing_id) {
      return $id;
    }

    // Reset the sequence to a higher value than the existing id.
    $this->query("DROP SEQUENCE " . $sequence_name);
    $this->query("CREATE SEQUENCE " . $sequence_name . " START WITH " . ($existing_id + 1));

    // Retrive the next id. We know this will be as high as we want it.
    $id = $this->query("SELECT " . $sequence_name . ".nextval FROM DUAL")->fetchField();

    return $id;
  }

  /**
   * Help method to check if array is associative.
   */
  public static function isAssoc($array) {
    return (is_array($array) && 0 !== count(array_diff_key($array, array_keys(array_keys($array)))));
  }

  public function makePrimary() {
    // We are installing a primary database.
    $this->external = FALSE;
  }

  public function oracleQuery($query, $args = NULL) {
    // Set a Fake Statement class.
    if (!empty($this->statementClass)) {
      $this->connection->setAttribute(\PDO::ATTR_STATEMENT_CLASS, array($this->statementClassOracle, array($this)));
    }

    $stmt = $this->prepare($query);

    try {
      $stmt->execute($args);
    }
    catch (\Exception $e) {
      syslog(LOG_ERR, "error: {$e->getMessage()} {$query}");
      throw $e;
    }

    // Set default Statement class.
    if (!empty($this->statementClass)) {
      $this->connection->setAttribute(\PDO::ATTR_STATEMENT_CLASS, array($this->statementClass, array($this)));
    }

    return $stmt;
  }

  private function exceptionQuery(&$unformattedQuery) {
    global $oracle_exception_queries;

    if (!is_array($oracle_exception_queries)) {
      return FALSE;
    }

    $count = 0;
    $oracle_unformatted_query = preg_replace(
      array_keys($oracle_exception_queries),
      array_values($oracle_exception_queries),
      $oracle_unformatted_query,
      -1,
      $count
    );

    return $count;
  }

  public function lastInsertId($name = NULL) {
    if (!$name) {
      throw new Exception('The name of the sequence is mandatory for Oracle');
    }

    try {
      return $this->oracleQuery($this->prefixTables("select " . $name . ".currval from dual", TRUE))->fetchColumn();
    }
    catch (\Exception $e) {
      // Ignore if CURRVAL not set (may be an insert that specified the serial field).
      syslog(LOG_ERR, " currval: " . print_r(debug_backtrace(FALSE), TRUE));
    }
  }

  public function generateTemporaryTableName() {
    // FIXME: create a cleanup job.
    return "TMP_" . $this->oracleQuery("SELECT userenv('sessionid') FROM dual")->fetchColumn() . "_" . $this->temporaryNameIndex++;
  }

  public function quote($string, $parameter_type = \PDO::PARAM_STR) {
    return "'" . str_replace("'", "''", $string) . "'";
  }

  public function version() {
    //try {
    //  return $this->getAttribute(\PDO::ATTR_SERVER_VERSION);
    //}
    //catch (\Exception $e) {
    //  return $this->oracleQuery("select regexp_replace(banner,'[^0-9\.]','') from v\$version where banner like 'CORE%'")->fetchColumn();
    //}
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

  public function checkDbPrefix($db_prefix) {
    if (empty($db_prefix)) {
      return;
    }
    if (!isset($this->oraclePrefix[$db_prefix])) {
      $this->oraclePrefix[$db_prefix] = $this->oracleQuery("select identifier.check_db_prefix(?) from dual", array($db_prefix))->fetchColumn();
    }

    return $this->oraclePrefix[$db_prefix];
  }

  public function prefixTables($sql, $quoted = FALSE) {
    $quote = '';
    $ret = '';

    if (!$quoted) {
      $quote = '"';
    }

    // Replace specific table prefixes first.
    foreach ($this->prefixes as $key => $val) {
      $dp = $this->checkDbPrefix($val);
      if (is_object($sql)) {
        $sql = $sql->getQueryString();
      }
      $sql = strtr($sql, array('{' . strtoupper($key) . '}' => $quote . (empty($dp) ? strtoupper($key) : strtoupper($dp) . '"."' . strtoupper($key)) . $quote));
    }

    $dp = $this->checkDbPrefix($this->tablePrefix());
    $ret = strtr($sql, array('{' => $quote . (empty($dp) ? '' : strtoupper($dp) . '"."'), '}' => $quote));

    return $this->escapeAnsi($ret);
  }

  public function prepareQuery($query) {
    $query = $this->escapeEmptyLiterals($query);
    $query = $this->escapeAnsi($query);
    if (!$this->external) {
      $query = $this->getLongIdentifiersHandler()->escapeLongIdentifiers($query);
    }
    $query = $this->escapeReserved($query);
    $query = $this->escapeCompatibility($query);
    $query = $this->prefixTables($query, TRUE);
    $query = $this->escapeIfFunction($query);
    return $this->prepare($query);
  }

  private function escapeAnsi($query) {
    if (preg_match("/^select /i", $query) && !preg_match("/^select(.*)from/ims", $query)) {
      $query .= ' FROM DUAL';
    }

    $search = array(
      '/("\w+?")/e',
      "/([^\s\(]+) & ([^\s]+) = ([^\s\)]+)/",
      "/([^\s\(]+) & ([^\s]+) <> ([^\s\)]+)/", // bitand
      '/^RELEASE SAVEPOINT (.*)$/',
      '/\((.*) REGEXP (.*)\)/',
    );

    $replace = array(
      "strtoupper('\\1')",
      "BITAND(\\1,\\2) = \\3",
      "BITAND(\\1,\\2) <> \\3",
      "begin null; end;",
      "REGEXP_LIKE(\\1,\\2)",
    );

    return str_replace('\\"', '"', preg_replace($search, $replace, $query));
  }

  private function escapeEmptyLiteral($match) {
    if ($match[0] == "''") {
      return "'" . ORACLE_EMPTY_STRING_REPLACER . "'";
    }
    else {
      return $match[0];
    }
  }

  private function escapeEmptyLiterals($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    return preg_replace_callback("/'.*?'/", array($this, 'escapeEmptyLiteral'), $query);
  }

  private function escapeIfFunction($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    return preg_replace("/IF\s*\((.*?),(.*?),(.*?)\)/", 'case when \1 then \2 else \3 end', $query);
  }

  private function escapeReserved($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    $ddl = !((boolean) preg_match('/^(select|insert|update|delete)/i', $query));

    $search = array(
      "/({)(\w+)(})/e", // escapes all table names
      "/({L#)([0-9]+)(})/e", // escapes long id
      "/(\:)(uid|session|file|access|mode|comment|desc|size|start|end|increment)/e",
      "/(<uid>|<session>|<file>|<access>|<mode>|<comment>|<desc>|<size>" . ($ddl ? '' : '|<date>') . ")/e",
      '/([\(\.\s,\=])(uid|session|file|access|mode|comment|desc|size' . ($ddl ? '' : '|date') . ')([,\s\=)])/e',
      '/([\(\.\s,])(uid|session|file|access|mode|comment|desc|size' . ($ddl ? '' : '|date') . ')$/e',
    );

    $replace = array(
      "'\"\\1'.strtoupper('\\2').'\\3\"'",
      "'\"\\1'.strtoupper('\\2').'\\3\"'",
      "'\\1'.'db_'.'\\2'.'\\3'", // @TODO: count arguments problem.
      "strtoupper('\"\\1\"')",
      "'\\1'.strtoupper('\"\\2\"').'\\3'",
      "'\\1'.strtoupper('\"\\2\"')",
    );

    return preg_replace($search, $replace, $query);
  }

  public function removeFromCachedStatements($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    $iquery = md5($this->prefixTables($query, TRUE));
    if (isset($this->preparedStatements[$iquery])) {
      unset($this->preparedStatements[$iquery]);
    }
  }

  private function escapeCompatibility($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    $search = array(
      "''||", // remove empty concatenations leaved by concatenate_bind_variables
      "||''",
      "IN ()", // translate 'IN ()' to '= NULL' they do not match anything anyway (always false)
      "IN  ()",
      '(FALSE)',
      'POW(',
      ") AS count_alias", // ugly hacks here
      '"{URL_ALIAS}" GROUP BY path',
      "ESCAPE '\\\\'",
      'SELECT CONNECTION_ID() FROM DUAL',
      'SHOW PROCESSLIST',
      'SHOW TABLES',
    );

    $replace = array(
      "",
      "",
      "= NULL",
      "= NULL",
      "(1=0)",
      "POWER(",
      ") count_alias",// ugly hacks replace strings here
      '"{URL_ALIAS}" GROUP BY SUBSTRING_INDEX(source, \'/\', 1)',
      "ESCAPE '\\'",
      'SELECT DISTINCT sid FROM v$mystat',
      'SELECT DISTINCT stat.sid, sess.process, sess.status, sess.username, sess.schemaname, sql.sql_text FROM v$mystat stat, v$session sess, v$sql sql WHERE sql.sql_id(+) = sess.sql_id AND sess.status = \'ACTIVE\' AND sess.type = \'USER\'',
      'SELECT * FROM user_tables',
    );

    return str_replace($search, $replace, $query);
  }

  public function makeSequenceName($table, $field) {
    $sequence_name = $this->schema()->oid('SEQ_' . $table . '_' . $field, FALSE, FALSE);
    return '"{' . $sequence_name . '}"';
  }

  public function cleanupArgValue($value) {
    if (is_string($value)) {
      if ($value == '') {
        return ORACLE_EMPTY_STRING_REPLACER;
      }
      elseif (strlen($value) > $this->max_varchar2_bind_size) {
        return $this->writeBlob($value);
      }
      else {
        return $value;
      }
    }
    else {
      return $value;
    }
  }

  public function cleanupArgs($args) {
    if ($this->external) {
      return $args;
    }

    $ret = array();
    if (Connection::isAssoc($args)) {
      foreach ($args as $key => $value) {
        $key = Connection::escapeReserved($key); // bind variables cannot have reserved names
        $key = $this->getLongIdentifiersHandler()->escapeLongIdentifiers($key);
        $ret[$key] = $this->cleanupArgValue($value);
      }
    }
    else { // indexed array
      foreach ($args as $key => $value) {
        $ret[$key] = $this->cleanupArgValue($value);
      }
    }

    return $ret;
  }

  public function writeBlob($value) {
    $hash = md5($value);
    $stmt = $this->connection->prepare("select blobid from blobs where hash = :hash");
    $stmt->bindParam(':hash', $hash, \PDO::PARAM_STR);
    $stmt->execute();
    $handle = $stmt->fetchColumn();

    if (empty($handle)) {
      $stream = Connection::stringToStream($value);
      $transaction = $this->startTransaction();
      $stmt = $this->prepareQuery("insert into blobs (blobid, content, hash) VALUES (seq_blobs.nextval, EMPTY_BLOB(), :hash) RETURNING content INTO :content");
      $stmt->bindParam(':hash', $hash, \PDO::PARAM_STR);
      $stmt->bindParam(':content', $stream, \PDO::PARAM_LOB);
      $stmt->execute();
      unset($transaction);
      $handle = $this->lastInsertId("seq_blobs");
    }

    $handle = ORACLE_BLOB_PREFIX . $handle;
    return $handle;
  }

  public function readBlob($handle) {
    $handle = (int) substr($handle, strlen(ORACLE_BLOB_PREFIX));
    $stmt = parent::prepare("select content from blobs where blobid= ?");
    $stmt->bindParam(1, $handle, \PDO::PARAM_INT);
    $stmt->execute();
    $return = $stmt->fetchColumn();

    if (!empty($return)) {
      return $return;
    }
    return '';
  }

  /**
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
  public function cleanupFetched($f) {
    if ($this->external) {
      return $f;
    }

    if (is_array($f)) {
      foreach ($f as $key => $value) {
        if ((string) $key == strtolower(ORACLE_ROWNUM_ALIAS)) {
          unset($f[$key]);
        }
        // Long identifier.
        elseif (Connection::isLongIdentifier($key)) {
          $f[$this->getLongIdentifiersHandler()->longIdentifierKey($key)] = $this->cleanupFetched($value);
          unset($f[$key]);
        }
        else {
          $f[$key] = $this->cleanupFetched($value);
        }
      }
    }
    elseif (is_object($f)) {
      foreach ($f as $key => $value) {
        if ((string) $key == strtolower(ORACLE_ROWNUM_ALIAS)) {
          unset($f->{$key});
        }
        // Long identifier.
        elseif (Connection::isLongIdentifier($key)) {
          $f->{$this->getLongIdentifiersHandler()->longIdentifierKey($key)} = $this->cleanupFetched($value);
          unset($f->{$key});
        }
        else {
          $f->{$key} = $this->cleanupFetched($value);
        }
      }
    }
    else {
      $f = $this->cleanupFetchedValue($f);
    }

    return $f;
  }

  public function cleanupFetchedValue($value) {
    if (is_string($value)) {
      if ($value == ORACLE_EMPTY_STRING_REPLACER) {
        return '';
      }
      elseif ($this->isBlob($value)) {
        return $this->readBlob($value);
      }
      else {
        return $value;
      }
    }
    else {
      return $value;
    }
  }

  public function resetLongIdentifiers() {
    if (!$this->external) {
      $this->getLongIdentifiersHandler()->resetLongIdentifiers();
    }
  }

  public static function isLongIdentifier($key) {
    return (substr(strtoupper($key), 0, strlen(ORACLE_LONG_IDENTIFIER_PREFIX)) == ORACLE_LONG_IDENTIFIER_PREFIX);
  }

  public static function isBlob($value) {
    return (substr($value, 0, strlen(ORACLE_BLOB_PREFIX)) == ORACLE_BLOB_PREFIX);
  }

  private static function stringToStream($value) {
    $stream = fopen('php://memory', 'a');
    fwrite($stream, $value);
    rewind($stream);
    return $stream;
  }

  /**
   * Long identifier support.
   */
  public function getLongIdentifiersHandler() {
    static $long_identifier = NULL;

    if ($this->external) {
      return NULL;
    }

    // Initialize the long identifier handler.
    if (empty($long_identifier)) {
      $long_identifier = new DatabaseLongIdentifierHandlerOracle($this);
    }
    return $long_identifier;
  }
}

/**
 * @TODO: remove this?
 */
class DatabaseLongIdentifierHandlerOracle {
  // Holds search reg exp pattern to match known long identifiers.
  private $searchLongIdentifiers = array();

  // Holds replacement string to replace known long identifiers.
  private $replaceLongIdentifiers = array();

  // Holds long identifier hashmap.
  private $hashLongIdentifiers = array();

  // The parent connection.
  private $connection;

  public function __construct($connection) {
    $this->connection = $connection;

    // Load long identifiers for the first time in this connection.
    $this->resetLongIdentifiers();
  }

  public function escapeLongIdentifiers($query) {
    $ret = "";

    // Do not replace things in literals.
    $literals = array();
    preg_match_all("/'.*?'/", $query, $literals);
    $literals    = $literals[0];
    $replaceable = preg_split("/'.*?'/", $query);
    $lidx        = 0;

    // Assume that a query cannot start with a literal and that.
    foreach ($replaceable as $toescape) {
      $ret .= $this->removeLongIdentifiers($toescape) . (isset($literals[$lidx]) ? $literals[$lidx++] : "");
    }
    return $ret;
  }

  public function removeLongIdentifiers($query_part) {
    if (count($this->searchLongIdentifiers)) {
      return preg_replace($this->searchLongIdentifiers, $this->replaceLongIdentifiers, $query_part);
    }
    else {
      return $query_part;
    }
  }

  public function resetLongIdentifiers() {
    // @TODO: would be wonderfull to enble a memcached switch here.
    try  {
      $result = $this->connection->oracleQuery("select id, identifier from long_identifiers where substr(identifier,1,3) not in ('IDX','TRG','PK_','UK_') order by length(identifier) desc");

      while ($row = $result->fetchObject()) {
        $this->searchLongIdentifiers[] = '/\b' . $row->identifier . '\b/i';
        $this->replaceLongIdentifiers[] = ORACLE_LONG_IDENTIFIER_PREFIX . $row->id;
        $this->hashLongIdentifiers[ORACLE_LONG_IDENTIFIER_PREFIX . $row->id] = strtolower($row->identifier);
      }
    }
    catch (\Exception $e) {
      // Ignore until long_identifiers table is not created.
    }
  }

  public function findAndRecordLongIdentifiers($query_part) {
    preg_match_all("/\w+/", $query_part, $words);
    $words = $words[0];
    foreach ($words as $word) {
      if (strlen($word) > ORACLE_IDENTIFIER_MAX_LENGTH) {
        $this->connection->schema()->oid($word);
      }
    }
  }

  public function findAndRemoveLongIdentifiers($query) {
    $this->connection->removeFromCachedStatements($query);

    // Do not replace things in literals.
    $literals = array();
    $replaceable = preg_split("/'.*?'/", $query);
    $lidx = 0;

    // Assume that a query cannot start with a literal and that.
    foreach ($replaceable as $toescape) {
      $this->findAndRecordLongIdentifiers($toescape);
    }
    $this->resetLongIdentifiers();
  }

  public function longIdentifierKey($key) {
    return $this->hashLongIdentifiers[strtoupper($key)];
  }

  /**
   * {@inheritdoc}
   */
  public function serialize() {
    return serialize(array());
  }
}