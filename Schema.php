<?php

namespace Drupal\Driver\Database\oracle;

use Drupal\Component\Utility\SafeMarkup;
use Drupal\Core\Database\DatabaseExceptionWrapper;
use Drupal\Core\Database\SchemaObjectExistsException;
use Drupal\Core\Database\SchemaObjectDoesNotExistException;
use Drupal\Core\Database\Schema as DatabaseSchema;

/**
 * Oracle implementation of \Drupal\Core\Database\Schema.
 */
class Schema extends DatabaseSchema {

 /**
   * A cache of information about blob columns and sequences of tables.
   *
   * This is collected by Schema::queryTableInformation(), by introspecting the
   * database.
   *
   * @see \Drupal\Core\Database\Driver\oracle\Schema::queryTableInformation()
   * @var array
   */
  protected $tableInformation = [];

  private $foundLongIdentifier = FALSE;

  /**
   * Oracle schema helper.
   */
  public function oid($name, $prefix = FALSE, $quote = TRUE) {
    $return = $name;

    if (strlen($return) > ORACLE_IDENTIFIER_MAX_LENGTH) {
      $this->foundLongIdentifier = TRUE;
      $return = $this->connection->oracleQuery('select identifier.get_for(?) from dual', array(strtoupper($return)))->fetchColumn();
    }

    $return = $prefix ? '{' . $return . '}' : strtoupper($return);

    if (!$prefix && $quote) {
      $return = '"' . $return . '"';
    }

    return $return;
  }

  /**
   * Oracle schema helper.
   */
  private function resetLongIdentifiers() {
    if ($this->foundLongIdentifier) {
      $this->connection->resetLongIdentifiers();
      $this->foundLongIdentifier = FALSE;
    }
  }

  /**
   * Fetch the list of blobs and sequences used on a table.
   *
   * We introspect the database to collect the information required by insert
   * and update queries.
   *
   * @param $table_name
   *   The non-prefixed name of the table.
   * @return
   *   An object with two member variables:
   *     - 'blob_fields' that lists all the blob fields in the table.
   *     - 'sequences' that lists the sequences used in that table.
   */
  public function queryTableInformation($table) {
    $key = $this->getTableInformationKey($table);

    $table_information = (object) [
      'blob_fields' => [],
      'sequences' => [],
    ];

    if (empty($this->tableInformation[$key])) {
      $table_name = strtoupper($this->connection->prefixTables('{' . $table . '}'));
      // @todo Support schema.
      $blobs = $this->connection->query("SELECT column_name FROM user_tab_columns WHERE data_type = 'BLOB' AND table_name = :db_table", [':db_table' => $table_name])
        ->fetchCol();
      $sequences = $this->connection->query("SELECT sequence_name FROM user_tab_identity_cols WHERE table_name = :db_table", [':db_table' => $table_name])
        ->fetchCol();

      $table_information->blob_fields = array_combine($blobs, $blobs);
      $table_information->sequences = $sequences;

      $this->tableInformation[$key] = $table_information;
    }

    return $this->tableInformation[$key];
  }

  /**
   * Resets information about table blobs, sequences and serial fields.
   *
   * @param $table
   *   The non-prefixed name of the table.
   */
  protected function resetTableInformation($table) {
    $key = $this->getTableInformationKey($table);
    unset($this->tableInformation[$key]);
  }

  /**
   * Returns the key for the tableInformation array for a given table.
   *
   * @param $table
   *   The non-prefixed name of the table.
   */
  protected function getTableInformationKey($table) {
    $key = $this->connection->prefixTables('{' . $table . '}');
    if (strpos($key, '.') === FALSE) {
      // @TODO Find current owner
      $key = 'public.' . $key;
    }
    $key = strtoupper(str_replace('"', '', $key));

    return $key;
  }

  /**
   * Emulates mysql default column behaviour.
   *
   * Eg. insert into table (col1) values (null).
   * If col1 has default in mysql you have the default inserted instead of null.
   * On oracle you have null inserted. So we need a trigger to intercept this
   * condition and substitute null with default. This happens on MySQL only
   * inserting not updating.
   */
  public function rebuildDefaultsTrigger($table) {
    $schema = $this->tableSchema($this->connection->prefixTables('{' . strtoupper($table) . '}', TRUE));
    $oname = $this->oid($table, FALSE, FALSE);

    $trigger = 'create or replace trigger ' . $this->oid('TRG_' . $table . '_DEFS', TRUE) .
      ' before insert on ' . $this->oid($table, TRUE) .
      ' for each row begin /* defs trigger */ if inserting then ';

    $serial_oname = $this->connection->oracleQuery("select field_name from table(identifier.get_serial(?,?))", array($table, $schema))->fetchColumn();
    $serial_oname = $serial_oname ? $serial_oname : "^NNC^";

    $stmt = $this->connection->oracleQuery(
      "select /*+ALL_ROWS*/ column_name,
       data_default
       from all_tab_columns
       where column_name != ?
       and owner= nvl(user,?)
       and table_name= ?
       and data_default is not null
      ",
      array($serial_oname, $schema, $oname)
    );

    $def = FALSE;

    while ($row = $stmt->fetchObject()) {
      $def = TRUE;
      $trigger .=
        'if :NEW."' . $row->column_name . '" is null or to_char(:NEW."' . $row->column_name . '") = \'' . ORACLE_EMPTY_STRING_REPLACER . '\'
          then :NEW."' . $row->column_name . '":= ' . $row->data_default . ';
          end if;
        ';
    }

    if (!$def) {
      $trigger .= ' null; ';
    }

    $trigger .= 'end if; end;';
    $this->connection->query($trigger);
  }

  /**
   * {@inheritdoc}
   */
  public function createTable($name, $table) {
    parent::createTable($name, $table);
    $this->rebuildDefaultsTrigger($name);
    $this->resetLongIdentifiers();
  }

  /**
   * Generate SQL to create a new table from a Drupal schema definition.
   *
   * @param string $name
   *   The name of the table to create.
   * @param array $table
   *   A Schema API table definition array.
   *
   * @return string
   *   An array of SQL statements to create the table.
   */
  protected function createTableSql($name, array $table) {
    $oname = $this->oid($name, TRUE);

    $sql_fields = array();
    foreach ($table['fields'] as $field_name => $field) {
      $sql_fields[] = $this->createFieldSql($field_name, $this->processField($field));
    }

    $sql_keys = array();

    if (isset($table['primary key']) && is_array($table['primary key'])) {
      $sql_keys[] = 'CONSTRAINT ' . $this->oid('PK_' . $name) . ' PRIMARY KEY (' . $this->createColsSql($table['primary key']) . ')';
    }

    if (isset($table['unique keys']) && is_array($table['unique keys'])) {
      foreach ($table['unique keys'] as $key_name => $key) {
        $sql_keys[] = 'CONSTRAINT ' . $this->oid('UK_' . $name . '_' . $key_name) . ' UNIQUE (' . $this->createColsSql($key) . ')';
      }
    }

    $sql = "CREATE TABLE " . $oname . " (\n\t" . implode(",\n\t", $sql_fields);
    if (count($sql_keys) > 0) {
      $sql .= ",\n\t";
    }
    $sql .= implode(",\n\t", $sql_keys) . "\n)";
    $statements[] = $sql;

    if (isset($table['indexes']) && is_array($table['indexes'])) {
      foreach ($table['indexes'] as $key_name => $key) {
        $statements = array_merge($statements, $this->createIndexSql($name, $key_name, $key));
      }
    }

    // Add table comment.
    if (isset($table['description'])) {
      $statements[] = 'COMMENT ON TABLE ' . $oname . ' IS ' . $this->prepareComment($table['description']);
    }

    // Add column comments.
    foreach ($table['fields'] as $field_name => $field) {
      if (isset($field['description'])) {
        $statements[] = 'COMMENT ON COLUMN ' . $oname . '.' . $this->oid($field_name) . ' IS ' . $this->prepareComment($field['description']);
      }
    }

    return $statements;
  }

  /**
   * Create an SQL string for a field.
   *
   * To be used in table creation or alteration. Before passing a field out of
   * a schema definition into this function it has to be processed by
   * Schema:processField().
   *
   * @param string $name
   *   Name of the field.
   * @param array $spec
   *   The field specification, as per the schema data structure format.
   *
   * @return string
   *   An array of SQL statements to create the field.
   */
  protected function createFieldSql($name, array $spec) {
    $oname = $this->oid($name);
    $sql = $oname . ' ' . $spec['oracle_type'];

    if (isset($spec['type']) && $spec['type'] == 'serial') {
      unset($spec['not null']);
    }

    if ($spec['oracle_type'] == 'VARCHAR2') {
      $sql .= '(' . (!empty($spec['length']) ? $spec['length'] : ORACLE_MAX_VARCHAR2_LENGTH) . ' CHAR)';
    }
    elseif (!empty($spec['length'])) {
      $sql .= '(' . $spec['length'] . ')';
    }
    elseif (isset($spec['precision']) && isset($spec['scale'])) {
      if ($spec['oracle_type'] == 'DOUBLE PRECISION' || $spec['oracle_type'] == 'FLOAT') {
        // For double and floats and precision and scale only NUMBER is supported.
        // @todo Check performance at some point.
        $sql = $oname . ' NUMBER';
      }
      $sql .= '(' . $spec['precision'] . ', ' . $spec['scale'] . ')';
    }

    if (isset($spec['identity'])) {
      $sql .= " GENERATED BY DEFAULT ON NULL AS IDENTITY";
    }
    elseif (isset($spec['default'])) {
      $default = is_string($spec['default']) ? $this->connection->quote($this->connection->cleanupArgValue($spec['default'])) : $spec['default'];
      $sql .= " default {$default}";
    }

    if (!empty($spec['not null'])) {
      $sql .= ' NOT NULL';
    }

    if (!empty($spec['unsigned'])) {
      $sql .= " CHECK ({$oname} >= 0)";
    }

    return $sql;
  }

  /**
   * Set database-engine specific properties for a field.
   *
   * @param array $field
   *   A field description array, as specified in the schema documentation.
   *
   * @return array
   *   A field description array after changes.
   */
  protected function processField(array $field) {
    if (!isset($field['size'])) {
      $field['size'] = 'normal';
    }

    // Set the correct database-engine specific datatype.
    // In case one is already provided, force it to uppercase.
    if (isset($field['oracle_type'])) {
      $field['oracle_type'] = drupal_strtoupper($field['oracle_type']);
    }
    else {
      $map = $this->getFieldTypeMap();
      $field['oracle_type'] = $map[$field['type'] . ':' . $field['size']];
    }

    if ($field['type'] == 'serial') {
      $field['identity'] = TRUE;
    }

    return $field;
  }

  /**
   * Returns Oracle specific field type infomration.
   */
  protected function getNumberPrecisionMap() {
    // @TODO Use this for serial and int.
    static $map = [
      'tiny'     => 3,
      'small'    => 5,
      'medium'   => 7,
      'big'      => 19,
      'normal'   => 10,
    ];

    return $map;
  }

  /**
   * {@inheritdoc}
   */
  public function getFieldTypeMap() {
    // Put :normal last so it gets preserved by array_flip. This makes
    // it much easier for modules (such as schema.module) to map
    // database types back into schema types.
    static $map = [
      'varchar_ascii:normal' => 'VARCHAR2',

      'varchar:normal'  => 'VARCHAR2',
      'char:normal'     => 'CHAR',

      'text:tiny'       => 'VARCHAR2',
      'text:small'      => 'VARCHAR2',
      'text:medium'     => 'CLOB',
      'text:big'        => 'CLOB',
      'text:normal'     => 'CLOB',

      'serial:tiny'     => 'NUMBER',
      'serial:small'    => 'NUMBER',
      'serial:medium'   => 'NUMBER',
      'serial:big'      => 'NUMBER',
      'serial:normal'   => 'NUMBER',

      'int:tiny'        => 'NUMBER',
      'int:small'       => 'NUMBER',
      'int:medium'      => 'NUMBER',
      'int:big'         => 'NUMBER',
      'int:normal'      => 'NUMBER',

      'float:tiny'      => 'FLOAT',
      'float:small'     => 'FLOAT',
      'float:medium'    => 'FLOAT',
      'float:big'       => 'DOUBLE PRECISION',
      'float:normal'    => 'FLOAT',

      'numeric:normal'  => 'DOUBLE PRECISION',

      'blob:big'        => 'BLOB',
      'blob:normal'     => 'BLOB',

      // @TODO Recheck this.
      'date:normal' => 'date',

      'datetime:normal' => 'timestamp with local time zone',
      'timestamp:normal' => 'timestamp',
      'time:normal'     => 'timestamp',
    ];

    return $map;
  }

  /**
   * {@inheritdoc}
   */
  public function renameTable($table, $new_name) {
    $oname = $this->oid($new_name, TRUE);

    // Should not use prefix because schema is not needed on rename.
    $this->connection->query('ALTER TABLE ' . $this->oid($table, TRUE) . ' RENAME TO ' . $this->oid($new_name, TRUE));

    // Rename indexes.
    $schema = $this->tableSchema($this->connection->prefixTables('{' . strtoupper($table) . '}', TRUE));
    if ($schema) {
      $stmt = $this->connection->query("SELECT nvl((select identifier from long_identifiers where 'L#'||to_char(id)= index_name),index_name) index_name FROM all_indexes WHERE table_name= ? and owner= ?", array(
        $this->oid($new_name, FALSE, FALSE),
        $schema,
      ));
    }
    else {
      $stmt = $this->connection->query("SELECT nvl((select identifier from long_identifiers where 'L#'||to_char(id)= index_name),index_name) index_name FROM user_indexes WHERE table_name= ?", array(
        $this->oid($new_name, FALSE, FALSE),
      ));
    }
    while ($row = $stmt->fetchObject()) {
      $this->connection->query('ALTER INDEX ' . $this->oid($row->index_name, TRUE) . ' RENAME TO ' . $this->oid(str_replace(strtoupper($table), strtoupper($new_name), $row->index_name), FALSE));
    }

    $this->cleanUpSchema($table, $new_name);
  }

  /**
   * {@inheritdoc}
   */
  public function dropTable($table) {
    if (!$this->tableExists($table)) {
      return FALSE;
    }

    $this->connection->query('DROP TABLE ' . $this->oid($table, TRUE) . ' CASCADE CONSTRAINTS PURGE');
    $this->resetTableInformation($table);
  }

  /**
   * {@inheritdoc}
   */
  public function addField($table, $field, $spec, $new_keys = array()) {
    $fixnull = FALSE;

    if (!empty($spec['not null']) && !isset($spec['default'])) {
      $fixnull = TRUE;
      $spec['not null'] = FALSE;
    }

    $query = 'ALTER TABLE ' . $this->oid($table, TRUE) . ' ADD (';
    $query .= $this->createFieldSql($field, $this->processField($spec)) . ')';

    $this->connection->query($query);

    if (isset($spec['initial'])) {
      $sql = 'UPDATE ' . $this->oid($table, TRUE) . ' SET ' . $this->oid($field) . ' = :initial_value';
      $result = $this->connection->query($sql, array('initial_value' => $spec['initial']));
    }

    if ($fixnull) {
      $this->connection->query("ALTER TABLE " . $this->oid($table, TRUE) . " MODIFY (" . $this->oid($field) . " NOT NULL)");
    }

    if (isset($new_keys)) {
      $this->createKeys($table, $new_keys);
    }

    // Add column comment.
    if (!empty($spec['description'])) {
      $this->connection->query('COMMENT ON COLUMN ' . $this->oid($table, TRUE) . '.' . $this->oid($field) . ' IS ' . $this->prepareComment($spec['description']));
    }

    if ($spec['type'] == 'serial') {
      $statements = $this->createSerialSql($table, $field);

      foreach ($statements as $statement) {
        $this->connection->query($statement);
      }
    }

    $this->cleanUpSchema($table);
  }

  /**
   * {@inheritdoc}
   */
  public function dropField($table, $field) {
    try {
      $this->connection->query('ALTER TABLE ' . $this->oid($table, TRUE) . ' DROP COLUMN ' . $this->oid($field));
      $this->cleanUpSchema($table);

      return TRUE;
    }
    catch (\Exception $e) {
      return FALSE;
    }
  }

  /**
   * {@inheritdoc}
   */
  public function fieldSetDefault($table, $field, $default) {
    if (is_null($default)) {
      $default = 'NULL';
    }
    else {
      $default = is_string($default) ? $this->connection->quote($this->connection->cleanupArgValue($default)) : $default;
    }

    $this->connection->query('ALTER TABLE ' . $this->oid($table, TRUE) . ' MODIFY (' . $this->oid($field) . ' DEFAULT ' . $default . ' )');
    $this->rebuildDefaultsTrigger($table);
  }

  /**
   * {@inheritdoc}
   */
  public function fieldSetNoDefault($table, $field) {
    $this->connection->query('ALTER TABLE ' . $this->oid($table, TRUE) . ' MODIFY (' . $this->oid($field) . ' DEFAULT NULL)');
    $this->rebuildDefaultsTrigger($table);
  }

  /**
   * {@inheritdoc}
   */
  public function addPrimaryKey($table, $fields) {
    $this->connection->query('ALTER TABLE ' . $this->oid($table, TRUE) . ' ADD CONSTRAINT ' . $this->oid('PK_' . $table) . ' PRIMARY KEY (' . $this->createColsSql($fields) . ')');
  }

  /**
   * {@inheritdoc}
   */
  public function dropPrimaryKey($table) {
    $this->connection->query('ALTER TABLE ' . $this->oid($table, TRUE) . ' DROP CONSTRAINT ' . $this->oid('PK_' . $table));
  }

  /**
   * {@inheritdoc}
   */
  public function addUniqueKey($table, $name, $fields) {
    $this->connection->query('ALTER TABLE ' . $this->oid($table, TRUE) . ' ADD CONSTRAINT ' . $this->oid('UK_' . $table . '_' . $name) . ' UNIQUE (' . $this->createColsSql($fields) . ')');
  }

  /**
   * {@inheritdoc}
   */
  public function dropUniqueKey($table, $name) {
    $this->connection->query('ALTER TABLE ' . $this->oid($table, TRUE) . ' DROP CONSTRAINT ' . $this->oid('UK_' . $table . '_' . $name));
  }

  /**
   * {@inheritdoc}
   */
  public function addIndex($table, $name, $fields, array $specs) {
    $sql = $this->createIndexSql($table, $name, $fields, $specs);
    foreach ($sql as $stmt) {
      $this->connection->query($stmt);
    }
  }

  /**
   * {@inheritdoc}
   */
  public function dropIndex($table, $name) {
    $this->connection->query('DROP INDEX ' . $this->oid('IDX_' . $table . '_' . $name, TRUE));
  }

  /**
   * {@inheritdoc}
   */
  public function changeField($table, $field, $field_new, $spec, $new_keys = array()) {
    $this->connection->query('ALTER TABLE ' . $this->oid($table, TRUE) . ' RENAME COLUMN ' . $this->oid($field) . ' TO ' . $this->oid($field . '_old'));
    $not_null = isset($spec['not null']) ? $spec['not null'] : FALSE;
    unset($spec['not null']);

    if (!array_key_exists('size', $spec)) {
      $spec['size'] = 'normal';
    }

    $this->addField($table, (string) $field_new, $spec);

    $map = $this->getFieldTypeMap();
    $this->connection->query('UPDATE ' . $this->oid($table, TRUE) . ' SET ' . $this->oid($field_new) . ' = ' . $this->oid($field . '_old'));

    if ($not_null) {
      $this->connection->query("ALTER TABLE " . $this->oid($table, TRUE) . ' MODIFY (' . $this->oid($field_new) . ' NOT NULL)');
    }

    $this->dropField($table, $field . '_old');

    if (isset($new_keys)) {
      $this->createKeys($table, $new_keys);
    }

    $this->cleanUpSchema($table);
  }

  /**
   * {@inheritdoc}
   */
  public function indexExists($table, $name) {
    $schema = $this->tableSchema($this->connection->prefixTables('{' . strtoupper($table) . '}', TRUE));

    $oname = $this->oid('IDX_' . $table . '_' . $name, FALSE, FALSE);

    if ($schema) {
      $retval = $this->connection->query("SELECT 1 FROM all_indexes WHERE index_name = ? and table_name= ? and owner= ?", array(
        $oname, $this->oid($table, FALSE, FALSE),
        $schema,
      ))->fetchField();
    }
    else {
      $retval = $this->connection->query("SELECT 1 FROM user_indexes WHERE index_name = ? and table_name= ?", array(
        $oname,
        $this->oid($table, FALSE, FALSE),
      ))->fetchField();
    }

    if ($retval) {
      return TRUE;
    }
    else {
      return FALSE;
    }
  }

  /**
   * Retrieve a table or column comment.
   */
  public function getComment($table, $column = NULL) {
    $schema = $this->tableSchema($this->connection->prefixTables('{' . strtoupper($table) . '}', TRUE));

    if ($schema) {
      if (isset($column)) {
        return $this->connection->query('select comments from all_col_comments where column_name = ? and table_name = ? and owner = ?', array(
          $this->oid($column, FALSE, FALSE),
          $this->oid($table, FALSE, FALSE),
          $schema,
        ))->fetchField();
      }
      return $this->connection->query('select comments from all_tab_comments where table_name = ? and owner = ?', array(
        $this->oid($table, FALSE, FALSE),
        $schema,
      ))->fetchField();
    }
    else {
      if (isset($column)) {
        return $this->connection->query('select comments from user_col_comments where column_name = ? and table_name = ?', array(
          $this->oid($column, FALSE, FALSE),
          $this->oid($table, FALSE, FALSE),
        ))->fetchField();
      }
      return $this->connection->query('select comments from user_tab_comments where table_name = ?', array(
        $this->oid($table, FALSE, FALSE),
      ))->fetchField();
    }
  }

  /**
   * {@inheritdoc}
   */
  public function tableExists($table) {
    $schema = $this->tableSchema($this->connection->prefixTables('{' . strtoupper($table) . '}', TRUE));

    if ($schema) {
      $retval = $this->connection->query("SELECT 1 FROM all_tables WHERE temporary= 'N' and table_name = ? and owner= ?", array(
        $this->oid($table, FALSE, FALSE),
        $schema,
      ))->fetchField();
    }
    else {
      $retval = $this->connection->query("SELECT 1 FROM user_tables WHERE temporary= 'N' and table_name = ?", array(
        $this->oid($table, FALSE, FALSE),
      ))->fetchField();
    }

    if ($retval) {
      return TRUE;
    }
    else {
      return FALSE;
    }
  }

  /**
   * {@inheritdoc}
   */
  public function fieldExists($table, $column) {
    $schema = $this->tableSchema($this->connection->prefixTables('{' . strtoupper($table) . '}', TRUE));

    if ($schema) {
      $retval = $this->connection->query("SELECT 1 FROM all_tab_columns WHERE column_name = ? and table_name = ? and owner= ?", array(
        $this->oid($column, FALSE, FALSE),
        $this->oid($table, FALSE, FALSE),
        $schema,
      ))->fetchField();
    }
    else {
      $retval = $this->connection->query('SELECT 1 FROM user_tab_columns WHERE column_name= ? and table_name = ?', array(
        $this->oid($column, FALSE, FALSE),
        $this->oid($table, FALSE, FALSE),
      ))->fetchField();
    }

    if ($retval) {
      return TRUE;
    }
    return FALSE;
  }

  /**
   * {@inheritdoc}
   */
  public function findTables($table_expression) {
    $schema = $this->tableSchema($table_expression);
    $table_expression = str_replace('"' . $schema . '"."', '', $table_expression);
    $table_expression = str_replace('"', '', $table_expression);
    $res = $this->connection->query("SELECT '\"'||owner||'\".\"'||table_name||'\"' tab FROM all_tables WHERE owner= ? and table_name LIKE ?", array($schema, strtoupper($table_expression)))->fetchAllKeyed(0, 0);

    return $res;
  }

  /**
   * {@inheritdoc}
   */
  public function copyTable($source, $destination) {
    if (!$this->tableExists($source)) {
      throw new SchemaObjectDoesNotExistException(SafeMarkup::format("Cannot copy @source to @destination: table @source doesn't exist.", array('@source' => $source, '@destination' => $destination)));
    }
    if ($this->tableExists($destination)) {
      throw new SchemaObjectExistsException(SafeMarkup::format("Cannot copy @source to @destination: table @destination already exists.", array('@source' => $source, '@destination' => $destination)));
    }

    throw new DatabaseExceptionWrapper('Not implemented, see https://drupal.org/node/2056133.');
  }

  /**
   * Oracle schema helper.
   */
  protected function createColsSql($cols) {
    $return = array();
    foreach ($cols as $col) {
      if (is_array($col)) {
        $return[] = $this->oid($col[0]);
      }
      else {
        $return[] = $this->oid($col);
      }
    }
    return implode(', ', $return);
  }

  /**
   * Oracle schema helper.
   */
  protected function createIndexSql($table, $name, $fields) {
    $oname = $this->oid('IDX_' . $table . '_' . $name, TRUE);

    $sql = array();
    // Oracle doesn't like multiple indexes on the same column list.
    $ret = $this->dropIndexByColsSql($table, $fields);

    if ($ret) {
      $sql[] = $ret;
    }

    // Suppose we try to create two indexes in the same create table command we
    // will silently fail the second.
    $query = "begin execute immediate 'CREATE INDEX " . $oname . " ON " . $this->oid($table, TRUE) . " (";
    $query .= $this->createKeySql($fields) . ")'; exception when others then if sqlcode = -1408 then null; else raise; end if; end;";
    $sql[] = $query;

    return $sql;
  }

  /**
   * Oracle schema helper.
   */
  public function dropIndexByColsSql($table, $fields) {
    $schema = $this->tableSchema($this->connection->prefixTables('{' . strtoupper($table) . '}', TRUE));
    $stmt = $this->connection->oracleQuery(
      "select i.index_name,
       e.column_expression exp,
       i.column_name col
       from all_ind_columns i,
       all_ind_expressions e
       where i.column_position= e.column_position (+)
       and i.index_owner = e.index_owner (+)
       and i.table_name = e.table_name (+)
       and i.index_name = e.index_name (+)
       and (i.index_name like 'IDX%' or i.index_name like '" . ORACLE_LONG_IDENTIFIER_PREFIX . "%')
       and i.table_name = ?
       and i.index_owner = ?
      ",
      array(strtoupper($table), $schema)
    );

    $idx = array();
    while ($row = $stmt->fetchObject()) {
      if (!isset($idx[$row->index_name])) {
        $idx[$row->index_name] = array();
      }
      $idx[$row->index_name][] = $row->exp ? $row->exp : $row->col;
    }

    $col = array();

    foreach ($fields as $field) {
      if (is_array($field)) {
        $col[] = 'SUBSTR(' . $this->oid($field[0]) . ',1,' . $field[1] . ')';
      }
      else {
        $col[] = $this->oid($field, FALSE, FALSE);
      }
    }

    foreach ($idx as $name => $value) {
      if (!count(array_diff($value, $col))) {
        return 'DROP INDEX "' . strtoupper($schema) . '"."' . strtoupper($name) . '"';
      }
    }

    return FALSE;
  }

  /**
   * Oracle schema helper.
   */
  protected function createKeySql($fields) {
    $ret = array();
    foreach ($fields as $field) {
      if (is_array($field)) {
        $ret[] = 'substr(' . $this->oid($field[0]) . ', 1, ' . $field[1] . ')';
      }
      else {
        $ret[] = $this->oid($field);
      }
    }
    return implode(', ', $ret);
  }

  /**
   * Oracle schema helper.
   */
  protected function createKeys($table, $new_keys) {
    if (isset($new_keys['primary key'])) {
      $this->addPrimaryKey($table, $new_keys['primary key']);
    }

    if (isset($new_keys['unique keys'])) {
      foreach ($new_keys['unique keys'] as $name => $fields) {
        $this->addUniqueKey($table, $name, $fields);
      }
    }

    if (isset($new_keys['indexes'])) {
      foreach ($new_keys['indexes'] as $name => $fields) {
        $this->addIndex($table, $name, $fields);
      }
    }
  }

  /**
   * Oracle schema helper.
   */
  public static function tableSchema($table) {
    $exp = explode('.', $table);

    if (count($exp) > 1) {
      return strtoupper(str_replace('"', '', $exp[0]));
    }
    return FALSE;
  }

  /**
   * Oracle schema helper.
   */
  private function cleanUpSchema($cache_table, $trigger_table = '') {
    if (!$trigger_table) {
      $trigger_table = $cache_table;
    }

    $this->resetLongIdentifiers();
    $this->resetTableInformation($cache_table);
    $this->rebuildDefaultsTrigger($trigger_table);
  }

  /**
   * Oracle schema helper.
   */
  private function failSafeDdl($ddl) {
    try {
      $this->connection->query($ddl);
    }
    catch (\Exception $e) {
      // Ignore.
    }
  }

}
