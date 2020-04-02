<?php

namespace Drupal\Driver\Database\oracle;

use Drupal\Core\Database\DatabaseExceptionWrapper;
use Drupal\Core\Database\IntegrityConstraintViolationException;
use Drupal\Core\Database\Database;
use Drupal\Core\Database\Connection as DatabaseConnection;

/**
 * Used to replace '' character in queries.
 */
define('ORACLE_EMPTY_STRING_REPLACER', '^');

/**
 * Maximum oracle identifier length (e.g. table names cannot exceed the length).
 *
 * @TODO: make dynamic. 30 is a limit for v11. In OD12+ has new limit of 128.
 */
define('ORACLE_IDENTIFIER_MAX_LENGTH', 128);

/**
 * Prefix used for long identifier keys.
 */
define('ORACLE_LONG_IDENTIFIER_PREFIX', 'L#');

/**
 * Maximum length (in bytes) for a string value in a table column in oracle.
 *
 * Affects schema.inc table creation.
 */
define('ORACLE_MAX_VARCHAR2_LENGTH', 4000);

/**
 * Alias used for queryRange filtering (we have to remove that from resultsets).
 */
define('ORACLE_ROWNUM_ALIAS', 'RWN_TO_REMOVE');

/**
 * @addtogroup database
 * @{
 */

/**
 * Oracle implementation of \Drupal\Core\Database\Connection.
 */
class Connection extends DatabaseConnection {

  protected $oracleReservedWords = [
    'ACCESS',
    'ADD',
    'ALL',
    'ALTER',
    'AND',
    'ANY',
    'AS',
    'ASC',
    'AUDIT',
    'BETWEEN',
    'BY',
    'CHAR',
    'CHECK',
    'CLUSTER',
    'COLUMN',
    'COLUMN_VALUE',
    'COMMENT',
    'COMPRESS',
    'CONNECT',
    'CREATE',
    'CURRENT',
    'DATE',
    'DECIMAL',
    'DEFAULT',
    'DELETE',
    'DESC',
    'DISTINCT',
    'DROP',
    'ELSE',
    'EXCLUSIVE',
    'EXISTS',
    'FILE',
    'FLOAT',
    'FOR',
    'FROM',
    'GRANT',
    'GROUP',
    'HAVING',
    'IDENTIFIED',
    'IMMEDIATE',
    'IN',
    'INCREMENT',
    'INDEX',
    'INITIAL',
    'INSERT',
    'INTEGER',
    'INTERSECT',
    'INTO',
    'IS',
    'LEVEL',
    'LIKE',
    'LOCK',
    'LONG',
    'MAXEXTENTS',
    'MINUS',
    'MLSLABEL',
    'MODE',
    'MODIFY',
    'NESTED_TABLE_ID',
    'NOAUDIT',
    'NOCOMPRESS',
    'NOT',
    'NOWAIT',
    'NULL',
    'NUMBER',
    'OF',
    'OFFLINE',
    'ON',
    'ONLINE',
    'OPTION',
    'OR',
    'ORDER',
    'PCTFREE',
    'PRIOR',
    'PUBLIC',
    'RAW',
    'RENAME',
    'RESOURCE',
    'REVOKE',
    'ROW',
    'ROWID',
    'ROWNUM',
    'ROWS',
    'SELECT',
    'SESSION',
    'SET',
    'SHARE',
    'SID',
    'SIZE',
    'SMALLINT',
    'START',
    'SUCCESSFUL',
    'SYNONYM',
    'SYSDATE',
    'TABLE',
    'THEN',
    'TO',
    'TRIGGER',
    'UID',
    'UNION',
    'UNIQUE',
    'UPDATE',
    'USER',
    'VALIDATE',
    'VALUES',
    'VARCHAR',
    'VARCHAR2',
    'VIEW',
    'WHENEVER',
    'WHERE',
    'WITH',
  ];

  /**
   * Error code for "Unknown database" error.
   */
  const DATABASE_NOT_FOUND = 0;

  /**
   * We are being use to connect to an external oracle database.
   *
   * @var bool
   */
  public $external = FALSE;

  /**
   * {@inheritdoc}
   */
  protected $statementClass = 'Drupal\Driver\Database\oracle\Statement';

  private $oraclePrefix = array();

  /**
   * {@inheritdoc}
   */
  public function __construct(\PDO $connection, array $connection_options = array()) {
    parent::__construct($connection, $connection_options);

    // This driver defaults to transaction support, except if explicitly
    // passed FALSE.
    $this->transactionSupport = !isset($connection_options['transactions']) || ($connection_options['transactions'] !== FALSE);

    // Transactional DDL is not available in Oracle.
    $this->transactionalDDLSupport = FALSE;

    // Needed by DatabaseConnection.getConnectionOptions.
    $this->connectionOptions = $connection_options;

    // Initialize db_prefix cache.
    $this->oraclePrefix = array();
  }

  /**
   * {@inheritdoc}
   */
  public static function open(array &$connection_options = array()) {
    // Default to TCP connection on port 1521.
    if (empty($connection_options['port'])) {
      $connection_options['port'] = 1521;
    }

    if ($connection_options['host'] === 'USETNS') {
      // Use database as TNSNAME.
      $dsn = 'oci:dbname=' . $connection_options['database'] . ';charset=AL32UTF8';
    }
    else {
      // Use host/port/database.
      $tns = "(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = {$connection_options['host']})(PORT = {$connection_options['port']})) (CONNECT_DATA = (SERVICE_NAME = {$connection_options['database']}) (SID = {$connection_options['database']})))";
      $dsn = "oci:dbname={$tns};charset=AL32UTF8";
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

  /**
   * {@inheritdoc}
   */
  public function query($query, array $args = array(), $options = array(), $retried = 0) {
    // Use default values if not already set.
    $options += $this->defaultOptions();

    try {
      if ($query instanceof \PDOStatement) {
        $stmt = $query;
      }
      else {
        $this->expandArguments($query, $args);
        $stmt = $this->prepareQuery($query);
        $args = $this->cleanupArgs($args);
      }

      $stmt->execute(empty($args) ? NULL : (array) $args, $options);

      switch ($options['return']) {
        case Database::RETURN_STATEMENT:
          return $stmt;

        case Database::RETURN_AFFECTED:
          $stmt->allowRowCount = TRUE;
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
        $message = $query_string . (isset($stmt) && $stmt instanceof Statement ? " (prepared: " . $stmt->getQueryString() . " )" : "") . " e: " . $e->getMessage() . " args: " . print_r($args, TRUE);
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

  /**
   * {@inheritdoc}
   */
  public function queryRange($query, $from, $count, array $args = array(), array $options = array()) {
    if (!$from) {
      $query .= sprintf(" FETCH FIRST %d ROWS ONLY", (int) $count);
    } else {
      $query .= sprintf(" OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", (int) $from, (int) $count);
    }

    return $this->query($query, $args, $options);
  }

  /**
   * {@inheritdoc}
   */
  public function queryTemporary($query, array $args = array(), array $options = array()) {
    $tablename = $this->generateTemporaryTableName();
    try {
      $this->query('DROP TABLE {' . $tablename . '}');
    }
    catch (\Exception $ex) {
      /* ignore drop errors */
    }
    $this->query('CREATE GLOBAL TEMPORARY TABLE {' . $tablename . '} ON COMMIT PRESERVE ROWS AS ' . $query, $args, $options);
    return $tablename;
  }

  /**
   * {@inheritdoc}
   */
  public function driver() {
    return 'oracle';
  }

  /**
   * {@inheritdoc}
   */
  public function databaseType() {
    return 'oracle';
  }

  /**
   * {@inheritdoc}
   */
  public function createDatabase($database) {
    // Database can be created manually only.
  }

  /**
   * {@inheritdoc}
   */
  public function mapConditionOperator($operator) {
    // We don't want to override any of the defaults.
    return NULL;
  }

  /**
   * {@inheritdoc}
   */
  public function nextId($existing_id = 0) {
    // Retrieve the name of the sequence. This information cannot be cached
    // because the prefix may change, for example, like it does in simpletests.
    $table_information = $this->schema()->queryTableInformation('sequences');
    $sequence_name = $table_information->sequences[0];

    $id = $this->query("SELECT " . $sequence_name . ".nextval FROM DUAL")->fetchField();
    if ($id > $existing_id) {
      return $id;
    }

    $new_id = ((int) $existing_id) + 1;

    // Reset the sequence to a higher value than the existing id.
    $this->query("ALTER TABLE {sequences} MODIFY (value GENERATED BY DEFAULT ON NULL AS IDENTITY START WITH $new_id)");

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

  /**
   * Oracle connection helper.
   */
  public function makePrimary() {
    // We are installing a primary database.
    $this->external = FALSE;
  }

  /**
   * Oracle connection helper.
   */
  public function oracleQuery($query, $args = NULL) {
    try {
      $stmt = $this->prepare($query);
      $stmt->execute($args);
      return $stmt;
    }
    catch (\Exception $e) {
      syslog(LOG_ERR, "error: {$e->getMessage()} {$query}");
      throw $e;
    }
  }

  /**
   * Oracle connection helper.
   */
  private function exceptionQuery(&$unformattedQuery) {
    global $_oracle_exception_queries;

    if (!is_array($_oracle_exception_queries)) {
      return FALSE;
    }

    $count = 0;
    $oracle_unformatted_query = preg_replace(
      array_keys($_oracle_exception_queries),
      array_values($_oracle_exception_queries),
      $oracle_unformatted_query,
      -1,
      $count
    );

    return $count;
  }

  /**
   * Oracle connection helper.
   */
  public function lastInsertId($name = NULL) {
    if (!$name) {
      throw new Exception('The name of the sequence is mandatory for Oracle');
    }

    try {
      return $this->oracleQuery($this->prefixTables("select " . $name . ".currval from dual", TRUE))->fetchColumn();
    }
    catch (\Exception $e) {
      // Ignore if CURRVAL not set. May be an insert that specified the serial
      // field.
      syslog(LOG_ERR, " currval: " . print_r(debug_backtrace(FALSE), TRUE));
    }
  }

  /**
   * {@inheritdoc}
   */
  public function generateTemporaryTableName() {
    // FIXME: create a cleanup job.
    return "TMP_" . $this->oracleQuery("SELECT userenv('sessionid') FROM dual")->fetchColumn() . "_" . $this->temporaryNameIndex++;
  }

  /**
   * {@inheritdoc}
   */
  public function quote($string, $parameter_type = \PDO::PARAM_STR) {
    return "'" . str_replace("'", "''", $string) . "'";
  }

  /**
   * {@inheritdoc}
   */
  public function version() {
    return NULL;
  }

  /**
   * Oracle connection helper.
   */
  public function checkDbPrefix($db_prefix) {
    if (empty($db_prefix)) {
      return;
    }
    if (!isset($this->oraclePrefix[$db_prefix])) {
      $this->oraclePrefix[$db_prefix] = $this->oracleQuery("select identifier.check_db_prefix(?) from dual", array($db_prefix))->fetchColumn();
    }

    return $this->oraclePrefix[$db_prefix];
  }

  /**
   * {@inheritdoc}
   */
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

  /**
   * Oracle connection helper.
   */
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

  /**
   * {@inheritdoc}
   */
  public function escapeField($field) {
    $escaped = parent::escapeField($field);

    // Remove any invalid start character.
    $escaped = preg_replace('/^[^A-Za-z0-9_]/', '', $escaped);

    // The pgsql database driver does not support field names that contain
    // periods (supported by PostgreSQL server) because this method may be
    // called by a field with a table alias as part of SQL conditions or
    // order by statements. This will consider a period as a table alias
    // identifier, and split the string at the first period.
    if (preg_match('/^([A-Za-z0-9_]+)"?[.]"?([A-Za-z0-9_.]+)/', $escaped, $parts)) {
      $table = $parts[1];
      $column = $parts[2];

      // Use escape alias because escapeField may contain multiple periods that
      // need to be escaped.
      $escaped = $this->escapeTable($table) . '.' . $this->escapeAlias($column);
    }
    else {
      $escaped = $this->doEscape($escaped);
    }

    return $escaped;
  }

  /**
   * {@inheritdoc}
   */
  public function escapeAlias($field) {
    $escaped = preg_replace('/[^A-Za-z0-9_]+/', '', $field);
    $escaped = $this->doEscape($escaped);
    return $escaped;
  }

  /**
   * Escape a string if needed.
   *
   * @param $string
   *   The string to escape.
   * @return string
   *   The escaped string.
   */
  protected function doEscape($string) {
    // Quote identifier to make it case-sensitive.
    // @todo Rework?
    if (preg_match('/[A-Z]/', $string)) {
      $string = '"' . $string . '"';
    }
    elseif (in_array(strtoupper($string), $this->oracleReservedWords)) {
      // Quote the string for Oracle reserved key words.
      $string = '"' . strtoupper($string) . '"';
    }
    return $string;
  }

  /**
   * Oracle connection helper.
   */
  private function escapeAnsi($query) {
    if (preg_match('/^select /i', $query) &&
      !preg_match('/^select(.*)from/ims', $query)) {
      $query .= ' FROM DUAL';
    }

    $search = array(
      "/([^\s\(]+) & ([^\s]+) = ([^\s\)]+)/",
      "/([^\s\(]+) & ([^\s]+) <> ([^\s\)]+)/",
      '/^RELEASE SAVEPOINT (.*)$/',
      '/\((.*) REGEXP (.*)\)/',
    );
    $replace = array(
      "BITAND(\\1,\\2) = \\3",
      "BITAND(\\1,\\2) <> \\3",
      'begin null; end;',
      "REGEXP_LIKE(\\1,\\2)",
    );
    $query = preg_replace($search, $replace, $query);

    $query = preg_replace_callback(
      '/("\w+?")/',
      function ($matches) {
        return strtoupper($matches[1]);
      },
      $query);

    return str_replace('\\"', '"', $query);
  }

  /**
   * Oracle connection helper.
   */
  private function escapeEmptyLiteral($match) {
    if ($match[0] == "''") {
      return "'" . ORACLE_EMPTY_STRING_REPLACER . "'";
    }
    else {
      return $match[0];
    }
  }

  /**
   * Oracle connection helper.
   */
  private function escapeEmptyLiterals($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    return preg_replace_callback("/'.*?'/", array($this, 'escapeEmptyLiteral'), $query);
  }

  /**
   * Oracle connection helper.
   */
  private function escapeIfFunction($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    return preg_replace("/IF\s*\((.*?),(.*?),(.*?)\)/", 'case when \1 then \2 else \3 end', $query);
  }

  /**
   * Oracle connection helper.
   */
  private function escapeReserved($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    $ddl = !((boolean) preg_match('/^(select|insert|update|delete)/i', $query));

    // Escapes all table names.
    $query = preg_replace_callback(
      '/({)(\w+)(})/',
      function ($matches) {
        return '"{' . strtoupper($matches[2]) . '}"';
      },
      $query);

    // Escapes long id.
    $query = preg_replace_callback(
      '/({L#)([\d]+)(})/',
      function ($matches) {
        return '"{L#' . strtoupper($matches[2]) . '}"';
      },
      $query);

    // Escapes reserved names.
    $query = preg_replace_callback(
      '/(\:)(uid|session|file|access|mode|comment|desc|size|start|end|increment)/',
      function ($matches) {
        return $matches[1] . 'db_' . $matches[2];
      },
      $query);

    $query = preg_replace_callback(
      '/(<uid>|<session>|<file>|<access>|<mode>|<comment>|<desc>|<size>' . ($ddl ? '' : '|<date>') . ')/',
      function ($matches) {
        return '"' . strtoupper($matches[1]) . '"';
      },
      $query);

    $query = preg_replace_callback(
      '/([\(\.\s,\=])(uid|session|file|access|mode|comment|desc|size' . ($ddl ? '' : '|date') . ')([,\s\=)])/',
      function ($matches) {
        return $matches[1] . '"' . strtoupper($matches[2]) . '"' . $matches[3];
      },
      $query);

    $query = preg_replace_callback(
      '/([\(\.\s,])(uid|session|file|access|mode|comment|desc|size' . ($ddl ? '' : '|date') . ')$/',
      function ($matches) {
        return $matches[1] . '"' . strtoupper($matches[2]) . '"';
      },
      $query);

    return $query;
  }

  /**
   * Oracle connection helper.
   */
  public function removeFromCachedStatements($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    $iquery = md5($this->prefixTables($query, TRUE));
    if (isset($this->preparedStatements[$iquery])) {
      unset($this->preparedStatements[$iquery]);
    }
  }

  /**
   * Oracle connection helper.
   */
  private function escapeCompatibility($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    $search = array(
      // Remove empty concatenations leaved by concatenate_bind_variables.
      "''||",
      "||''",

      // Translate 'IN ()' to '= NULL' they do not match anything anyway.
      "IN ()",
      "IN  ()",

      '(FALSE)',
      'POW(',
      ") AS count_alias",
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
      ") count_alias",
      '"{URL_ALIAS}" GROUP BY SUBSTRING_INDEX(source, \'/\', 1)',
      "ESCAPE '\\'",
      'SELECT DISTINCT sid FROM v$mystat',
      'SELECT DISTINCT stat.sid, sess.process, sess.status, sess.username, sess.schemaname, sql.sql_text FROM v$mystat stat, v$session sess, v$sql sql WHERE sql.sql_id(+) = sess.sql_id AND sess.status = \'ACTIVE\' AND sess.type = \'USER\'',
      'SELECT * FROM user_tables',
    );

    return str_replace($search, $replace, $query);
  }

  /**
   * {@inheritdoc}
   */
  public function makeSequenceName($table, $field) {
    $sequence_name = $this->schema()->oid('SEQ_' . $table . '_' . $field, FALSE, FALSE);
    return '"{' . $sequence_name . '}"';
  }

  /**
   * Oracle connection helper.
   */
  public function cleanupArgValue($value) {
    if ($value === '') {
      return ORACLE_EMPTY_STRING_REPLACER;
    }
    return $value;
  }

  /**
   * Oracle connection helper.
   */
  public function cleanupArgs($args) {
    if ($this->external) {
      return $args;
    }

    $ret = array();
    if (Connection::isAssoc($args)) {
      foreach ($args as $key => $value) {
        $key = Connection::escapeReserved($key);

        // Bind variables cannot have reserved names.
        $key = $this->getLongIdentifiersHandler()->escapeLongIdentifiers($key);
        $ret[$key] = $this->cleanupArgValue($value);

        // MW: Replace empty strings. Fixed config->get().
        $ret[$key] = $value === '' ? ORACLE_EMPTY_STRING_REPLACER : $value;
      }
    }
    else {
      // Indexed array.
      foreach ($args as $key => $value) {
        $ret[$key] = $this->cleanupArgValue($value);
      }
    }

    return $ret;
  }

  public function addSavepoint() {
  }

  public function releaseSavepoint() {
  }

  public function rollbackSavepoint() {
  }

  /**
   * Cleaned query string.
   *
   * 1) Long identifiers placeholders.
   *  May occur in queries like:
   *               select 1 as myverylongidentifier from mytable
   *  this is translated on query submission as e.g.:
   *               select 1 as L#321 from mytable
   *  so when we fetch this object (or array) we will have
   *     stdClass ( "L#321" => 1 ) or Array ( "L#321" => 1 ).
   *  but the code is expecting to access the field as myverylongidentifier,
   *  so we need to translate the "L#321" back to "myverylongidentifier".
   *
   * 2) BLOB placeholders.
   *   We can find values like B^#2354, and we have to translate those values
   *   back to their original long value so we read blob id 2354 of table blobs.
   *
   * 3) Removes the rwn column from queryRange queries.
   *
   * 4) Translate empty string replacement back to empty string.
   *
   * @return string
   *   Cleaned string to be executed.
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

  /**
   * Oracle connection helper.
   */
  public function cleanupFetchedValue($value) {
    if (is_string($value)) {
      if ($value == ORACLE_EMPTY_STRING_REPLACER) {
        return '';
      }
      else {
        return $value;
      }
    }
    else {
      return $value;
    }
  }

  /**
   * Oracle connection helper.
   */
  public function resetLongIdentifiers() {
    if (!$this->external) {
      $this->getLongIdentifiersHandler()->resetLongIdentifiers();
    }
  }

  /**
   * Oracle connection helper.
   */
  public static function isLongIdentifier($key) {
    return (substr(strtoupper($key), 0, strlen(ORACLE_LONG_IDENTIFIER_PREFIX)) == ORACLE_LONG_IDENTIFIER_PREFIX);
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
      $long_identifier = new LongIdentifierHandler($this);
    }
    return $long_identifier;
  }

}


/**
 * @} End of "addtogroup database".
 */
