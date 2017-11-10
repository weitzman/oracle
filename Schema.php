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
   * Cache table information used for InsertQuery and UpdateQuery.
   */
  private static $tableInformation = array();

  private $foundLongIdentifier = FALSE;

  public function oid($name, $prefix = FALSE, $quote = TRUE) {
    $return = $name;

    if (strlen($return) > ORACLE_IDENTIFIER_MAX_LENGTH) {
      $this->foundLongIdentifier = TRUE;
      $return = $this->connection->oracleQuery("select identifier.get_for(?) from dual",array(strtoupper($return)))->fetchColumn();
    }

    $return = $prefix ? '{' . $return . '}' : strtoupper($return);

    if (!$prefix && $quote) {
      $return = '"' . $return . '"';
    }

    return $return;
  }

  private function resetLongIdentifiers() {
    if ($this->foundLongIdentifier) {
      $this->connection->resetLongIdentifiers();
      $this->foundLongIdentifier = FALSE;
    }
  }

  public function getTableInfo($table) {
    $schema = $this->tableSchema($this->connection->prefixTables('{' . strtoupper($table) . '}', TRUE));

    $info = new \stdClass();
    try {
      if (!isset(Schema::$tableInformation[$schema . '|' . $table])) {
        $info->sequence_name = $this->connection->oracleQuery("select identifier.sequence_for_table(?,?) sequence_name from dual", array(strtoupper($table), $schema))->fetchColumn();
        Schema::$tableInformation[$schema . '|' . $table] = $info;
      }
      else {
        $info = Schema::$tableInformation[$schema . '|' . $table];
      }
    }
    catch (\PDOException $e) {
      if ($e->errorInfo[1] == '00904') {
        // Ignore (may be a connection to a non drupal schema not having the identifier pkg).
        // See http://drupal.org/node/1121044.
      }
      else {
        throw $e;
      }
    }

    return $info;
  }

  public function removeTableInfoCache($table) {
    $schema = $this->tableSchema($this->connection->prefixTables('{' . strtoupper($table) . '}', TRUE));
    unset(Schema::$tableInformation[$schema . '|' . $table]);
  }

  /**
   * Emulates mysql default column behaviour (eg.
   * insert into table (col1) values (null)
   * if col1 has default in mysql you have the default insterted instead of null.
   * On oracle you have null inserted.
   * So we need a trigger to intercept this condition and substitute null with default...
   * This condition happens on MySQL only inserting not updating.
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

    $trigger.= 'end if; end;';
    $this->connection->query($trigger);
  }

  public function createTable($name, $table) {
    parent::createTable($name, $table);
    $this->rebuildDefaultsTrigger($name);
    $this->resetLongIdentifiers();
  }

  public function createSerialSql($table, $field_name, $start_with = 1) {
    $oname = $this->oid($table, TRUE);

    $trgname = $this->oid('TRG_' . $table . '_' . $field_name, TRUE);
    $seqname = $this->oid('SEQ_' . $table . '_' . $field_name, TRUE);

    $ofield_name = $this->oid($field_name);

    $tblname_ser = $this->oid($table, FALSE, FALSE);
    $trgname_ser = $this->oid('TRG_' . $table . '_' . $field_name, FALSE, FALSE);
    $seqname_ser = $this->oid('SEQ_' . $table . '_' . $field_name, FALSE, FALSE);
    $fldname_ser = $this->oid($field_name, FALSE, FALSE);

    $statements[] = 'CREATE SEQUENCE ' . $seqname . ($start_with != 1 ? ' START WITH ' . $start_with : '');
    $statements[] = 'CREATE OR REPLACE TRIGGER ' . $trgname . ' before insert on ' . $oname . ' for each row declare v_id number:= 0; begin /* serial(' . $tblname_ser . ',' . $trgname_ser . ',' . $seqname_ser . ',' . $fldname_ser . ') */ if inserting then if :NEW.' . $ofield_name . ' is null or :NEW.' . $ofield_name .' = 0 then select ' . $seqname . '.nextval into :NEW.' . $ofield_name . ' from dual; else while v_id < :NEW.' . $ofield_name . ' loop select ' . $seqname . '.nextval into v_id from dual; end loop; end if; end if; end;';
    $statements[] = 'ALTER TRIGGER ' . $trgname .' ENABLE';

    return $statements;
  }

  /**
   * Generate SQL to create a new table from a Drupal schema definition.
   *
   * @param $name
   *   The name of the table to create.
   * @param $table
   *   A Schema API table definition array.
   * @return
   *   An array of SQL statements to create the table.
   */
  protected function createTableSql($name, $table) {
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

    foreach ($table['fields'] as $field_name => $field) {
      if ($field['type'] == 'serial') {
        $statements = array_merge($statements, $this->createSerialSql($name, $field_name));
      }
      elseif ($field['type'] == 'blob') {
        $statements[] = "INSERT INTO BLOB_COLUMN VALUES ('" . strtoupper($name) . "','" . strtoupper($field_name) . "')";
      }
    }

    return $statements;
  }

  /**
   * Create an SQL string for a field to be used in table creation or
   * alteration.
   *
   * Before passing a field out of a schema definition into this
   * function it has to be processed by Schema:processField().
   *
   * @param $name
   *    Name of the field.
   * @param $spec
   *    The field specification, as per the schema data structure format.
   */
  protected function createFieldSql($name, $spec) {
    $oname = $this->oid($name);
    $sql = $oname . ' ' . $spec['oracle_type'];

    if ($spec['type'] == 'serial') {
      unset($spec['not null']);
    }

    if ($spec['oracle_type'] == 'varchar2') {
      $sql .= '(' . (!empty($spec['length']) ? $spec['length'] : ORACLE_MAX_VARCHAR2_LENGTH) . ' CHAR)';
    }
    elseif (!empty($spec['length'])) {
      $sql .= '(' . $spec['length'] . ')';
    }
    elseif (isset($spec['precision']) && isset($spec['scale'])) {
      $sql .= '(' . $spec['precision'] . ', ' . $spec['scale'] . ')';
    }

    if (isset($spec['default'])) {
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
   * @param $field
   *   A field description array, as specified in the schema documentation.
   */
  protected function processField($field) {
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

    return $field;
  }

  /**
   * This maps a generic data type in combination with its data size
   * to the engine-specific data type.
   */
  public function getFieldTypeMap() {
    // Put :normal last so it gets preserved by array_flip. This makes
    // it much easier for modules (such as schema.module) to map
    // database types back into schema types.
    $map = array(
      'varchar_ascii:normal' => 'varchar2',

      'varchar:normal' => 'varchar2',
      'char:normal' => 'char',

      'text:tiny' => 'varchar2',
      'text:small' => 'varchar2',
      'text:medium' => 'varchar2',
      'text:big' => 'varchar2',
      'text:normal' => 'varchar2',

      'int:tiny' => 'number',
      'int:small' => 'number',
      'int:medium' => 'number',
      'int:big' => 'number',
      'int:normal' => 'number',

      'float:tiny' => 'number',
      'float:small' => 'number',
      'float:medium' => 'number',
      'float:big' => 'number',
      'float:normal' => 'number',

      'numeric:normal' => 'number',

      'blob:big' => 'varchar2',
      'blob:normal' => 'varchar2',

      'date:normal' => 'date',

      'datetime:normal' => 'timestamp with local time zone',
      'timestamp:normal' => 'timestamp',
      'time:normal'     => 'timestamp',

      'serial:tiny' => 'number',
      'serial:small' => 'number',
      'serial:medium' => 'number',
      'serial:big' => 'number',
      'serial:normal' => 'number',
    );

    return $map;
  }

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

  private function getTableSerialInfo($table) {
    $schema = $this->tableSchema($this->connection->prefixTables('{' . strtoupper($table) . '}', TRUE));
    return $this->connection->query("select * from table(identifier.get_serial(?, ?))", array(strtoupper($table), $schema))->fetchObject();
  }

  /**
   * Rename a table.
   *
   * @param $table
   *   The table to be renamed.
   * @param $new_name
   *   The new name for the table.
   */
  public function renameTable($table, $new_name) {
    $info = $this->getTableSerialInfo($table);
    $oname = $this->oid($new_name, TRUE);

    if (!empty($info->sequence_name)) {
      $this->failsafeDdl('DROP TRIGGER {' . $info->trigger_name . '}');
      $this->failsafeDdl('DROP SEQUENCE {' . $info->sequence_name . '}');
    }

    // Drop defaults trigger.
    $this->failsafeDdl("DROP TRIGGER " . $this->oid('TRG_' . $table . '_DEFS', TRUE));

    // Should not use prefix because schema is not needed on rename.
    $this->connection->query('ALTER TABLE ' . $this->oid($table, TRUE) . ' RENAME TO ' . $this->oid($new_name, FALSE));

    if (!empty($info->sequence_name)) {
      $statements = $this->createSerialSql($table, $info->field_name, $info->sequence_restart);
      foreach ($statements as $statement) {
        $this->connection->query($statement);
      }
    }

    // Rename indexes.
    $schema = $this->tableSchema($this->connection->prefixTables('{' . strtoupper($table) . '}', TRUE));
    if ($schema) {
      $stmt = $this->connection->query("SELECT nvl((select identifier from long_identifiers where 'L#'||to_char(id)= index_name),index_name) index_name FROM all_indexes WHERE table_name= ? and owner= ?", array($this->oid($new_name, FALSE, FALSE), $schema));
    }
    else {
      $stmt = $this->connection->query("SELECT nvl((select identifier from long_identifiers where 'L#'||to_char(id)= index_name),index_name) index_name FROM user_indexes WHERE table_name= ?", array($this->oid($new_name, FALSE, FALSE)));
    }
    while ($row = $stmt->fetchObject()) {
      $this->connection->query('ALTER INDEX ' . $this->oid($row->index_name, TRUE) . ' RENAME TO ' . $this->oid(str_replace(strtoupper($table), strtoupper($new_name), $row->index_name), FALSE));
    }

    $this->cleanUpSchema($table, $new_name);
  }

  /**
   * Drop a table.
   *
   * @param $table
   *   The table to be dropped.
   */
  public function dropTable($table) {
    $info = $this->getTableInfo($table);

    if ($info->sequence_name) {
      $this->connection->query('DROP SEQUENCE ' . $info->sequence_name);
    }

    if (!$this->tableExists($table)) {
      return FALSE;
    }

    $this->connection->query('DROP TABLE ' . $this->oid($table, TRUE) . ' CASCADE CONSTRAINTS PURGE');
    $this->removeTableInfoCache($table);
  }

  /**
   * Add a new field to a table.
   *
   * @param $table
   *   Name of the table to be altered.
   * @param $field
   *   Name of the field to be added.
   * @param $spec
   *   The field specification array, as taken from a schema definition.
   *   The specification may also contain the key 'initial', the newly
   *   created field will be set to the value of the key in all rows.
   *   This is most useful for creating NOT NULL columns with no default
   *   value in existing tables.
   * @param $keys_new
   *   Optional keys and indexes specification to be created on the
   *   table along with adding the field. The format is the same as a
   *   table specification but without the 'fields' element. If you are
   *   adding a type 'serial' field, you MUST specify at least one key
   *   or index including it in this array. @see Schema::changeField() for more
   *   explanation why.
   */
  public function addField($table, $field, $spec, $new_keys = array()) {
    $fixnull = FALSE;

    if (!empty($spec['not null']) && !isset($spec['default'])) {
      $fixnull = TRUE;
      $spec['not null'] = FALSE;
    }

    $query  = 'ALTER TABLE ' . $this->oid($table, TRUE) . ' ADD (';
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
      $statements = $this->createSerialSql($table,$field);

      foreach ($statements as $statement) {
        $this->connection->query($statement);
      }
    }

    $this->cleanUpSchema($table);
  }

  /**
   * Drop a field.
   *
   * @param $table
   *   The table to be altered.
   * @param $field
   *   The field to be dropped.
   */
  public function dropField($table, $field) {
    $info = $this->getTableSerialInfo($table);
    if (!empty($info->sequence_name) && $this->oid($field, FALSE, FALSE) == $info->field_name) {
      $this->failsafeDdl('DROP TRIGGER {' . $info->trigger_name . '}');
      $this->failsafeDdl('DROP SEQUENCE {' . $info->sequence_name . '}');
    }

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
   * Set the default value for a field.
   *
   * @param $table
   *   The table to be altered.
   * @param $field
   *   The field to be altered.
   * @param $default
   *   Default value to be set. NULL for 'default NULL'.
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
   * Set a field to have no default value.
   *
   * @param $table
   *   The table to be altered.
   * @param $field
   *   The field to be altered.
   */
  public function fieldSetNoDefault($table, $field) {
    $this->connection->query('ALTER TABLE ' . $this->oid($table, TRUE) . ' MODIFY (' . $this->oid($field) . ' DEFAULT NULL)');
    $this->rebuildDefaultsTrigger($table);
  }

  /**
   * Add a primary key.
   *
   * @param $table
   *   The table to be altered.
   * @param $fields
   *   Fields for the primary key.
   */
  public function addPrimaryKey($table, $fields) {
    $this->connection->query('ALTER TABLE ' . $this->oid($table, TRUE) . ' ADD CONSTRAINT ' . $this->oid('PK_' . $table) . ' PRIMARY KEY (' . $this->createColsSql($fields) . ')');
  }

  /**
   * Drop the primary key.
   *
   * @param $table
   *   The table to be altered.
   */
  public function dropPrimaryKey($table) {
    $this->connection->query('ALTER TABLE ' . $this->oid($table, TRUE) . ' DROP CONSTRAINT ' . $this->oid('PK_' . $table));
  }

  /**
   * Add a unique key.
   *
   * @param $table
   *   The table to be altered.
   * @param $name
   *   The name of the key.
   * @param $fields
   *   An array of field names.
   */
  public function addUniqueKey($table, $name, $fields) {
    $this->connection->query('ALTER TABLE ' . $this->oid($table, TRUE) . ' ADD CONSTRAINT ' . $this->oid('UK_' . $table . '_' . $name) . ' UNIQUE (' . $this->createColsSql($fields) . ')');
  }

  /**
   * Drop a unique key.
   *
   * @param $table
   *   The table to be altered.
   * @param $name
   *   The name of the key.
   */
  public function dropUniqueKey($table, $name) {
    $this->connection->query('ALTER TABLE ' . $this->oid($table, TRUE) . ' DROP CONSTRAINT ' . $this->oid('UK_' . $table . '_' . $name));
  }

  /**
   * Add an index.
   *
   * @param $table
   *   The table to be altered.
   * @param $name
   *   The name of the index.
   * @param $fields
   *   An array of field names.
   */
  public function addIndex($table, $name, $fields, array $specs) {
    $sql = $this->createIndexSql($table, $name, $fields, $specs);
    foreach ($sql as $stmt) {
      $this->connection->query($stmt);
    }
  }

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
   * Drop an index.
   *
   * @param $table
   *   The table to be altered.
   * @param $name
   *   The name of the index.
   */
  public function dropIndex($table, $name) {
    $this->connection->query('DROP INDEX ' . $this->oid('IDX_' . $table . '_' . $name, TRUE));
  }

  /**
   * Change a field definition.
   *
   * IMPORTANT NOTE: To maintain database portability, you have to explicitly
   * recreate all indices and primary keys that are using the changed field.
   *
   * That means that you have to drop all affected keys and indexes with
   * db_drop_{primary_key,unique_key,index}() before calling db_change_field().
   * To recreate the keys and indices, pass the key definitions as the
   * optional $new_keys argument directly to db_change_field().
   *
   * For example, suppose you have:
   * @code
   * $schema['foo'] = array(
   *   'fields' => array(
   *     'bar' => array('type' => 'int', 'not null' => TRUE)
   *   ),
   *   'primary key' => array('bar')
   * );
   * @endcode
   * and you want to change foo.bar to be type serial, leaving it as the
   * primary key. The correct sequence is:
   * @code
   * db_drop_primary_key( 'foo');
   * db_change_field('foo', 'bar', 'bar',
   *   array('type' => 'serial', 'not null' => TRUE),
   *   array('primary key' => array('bar')));
   * @endcode
   *
   * The reasons for this are due to the different database engines:
   *
   * On Oracle, changing a field definition involves adding a new field
   * and dropping an old one which* causes any indices, primary keys and
   * sequences (from serial-type fields) that use the changed field to be dropped.
   *
   * On MySQL, all type 'serial' fields must be part of at least one key
   * or index as soon as they are created. You cannot use
   * db_add_{primary_key,unique_key,index}() for this purpose because
   * the ALTER TABLE command will fail to add the column without a key
   * or index specification. The solution is to use the optional
   * $new_keys argument to create the key or index at the same time as
   * field.
   *
   * You could use db_add_{primary_key,unique_key,index}() in all cases
   * unless you are converting a field to be type serial. You can use
   * the $new_keys argument in all cases.
   *
   * @param $table
   *   Name of the table.
   * @param $field
   *   Name of the field to change.
   * @param $field_new
   *   New name for the field (set to the same as $field if you don't want to change the name).
   * @param $spec
   *   The field specification for the new field.
   * @param $new_keys
   *   Optional keys and indexes specification to be created on the
   *   table along with changing the field. The format is the same as a
   *   table specification but without the 'fields' element.
   */
  public function changeField($table, $field, $field_new, $spec, $new_keys = array()) {
    $info = $this->getTableSerialInfo($table);

    if (!empty($info->sequence_name) && $this->oid($field, FALSE, FALSE) == $info->field_name) {
      $this->failsafeDdl('DROP TRIGGER {' . $info->trigger_name . '}');
      $this->failsafeDdl('DROP SEQUENCE {' . $info->sequence_name . '}');
    }

    $this->connection->query("ALTER TABLE " . $this->oid($table, TRUE) . " RENAME COLUMN ". $this->oid($field) . " TO " . $this->oid($field . '_old'));
    $not_null = isset($spec['not null']) ? $spec['not null'] : FALSE;
    unset($spec['not null']);

    if (!array_key_exists('size', $spec)) {
      $spec['size'] = 'normal';
    }

    $this->addField($table, (string) $field_new, $spec);

    $map = $this->getFieldTypeMap();
    $this->connection->query("UPDATE " . $this->oid($table, TRUE) . " SET ". $this->oid($field_new) . " = " . $this->oid($field . '_old'));

    if ($not_null) {
      $this->connection->query("ALTER TABLE " . $this->oid($table, TRUE) . " MODIFY (". $this->oid($field_new) . " NOT NULL)");
    }

    $this->dropField($table, $field . '_old');

    if (isset($new_keys)) {
      $this->createKeys($table, $new_keys);
    }

    if (!empty($info->sequence_name) && $this->oid($field, FALSE, FALSE) == $info->field_name) {
      $statements = $this->createSerialSql($table, $this->oid($field_new, FALSE, FALSE), $info->sequence_restart);
      foreach ($statements as $statement) {
        $this->connection->query($statement);
      }
    }

    $this->cleanUpSchema($table);
  }

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

  public function indexExists($table, $name) {
    $schema = $this->tableSchema($this->connection->prefixTables('{' . strtoupper($table) . '}', TRUE));

    $oname = $this->oid('IDX_' . $table . '_' . $name, FALSE, FALSE);

    if ($schema) {
      $retval = $this->connection->query("SELECT 1 FROM all_indexes WHERE index_name = ? and table_name= ? and owner= ?", array($oname, $this->oid($table, FALSE, FALSE), $schema))->fetchField();
    }
    else {
      $retval = $this->connection->query("SELECT 1 FROM user_indexes WHERE index_name = ? and table_name= ?", array($oname, $this->oid($table, FALSE, FALSE)))->fetchField();
    }

    if ($retval) {
      return TRUE;
    }
    else {
      return FALSE;
    }
  }

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
   * Retrieve a table or column comment.
   */
  public function getComment($table, $column = NULL) {
    $schema = $this->tableSchema($this->connection->prefixTables('{' . strtoupper($table) . '}', TRUE));

    if ($schema) {
      if (isset($column)) {
        return $this->connection->query('select comments from all_col_comments where column_name = ? and table_name = ? and owner = ?', array($this->oid($column, FALSE, FALSE), $this->oid($table, FALSE, FALSE), $schema))->fetchField();
      }
      return $this->connection->query('select comments from all_tab_comments where table_name = ? and owner = ?', array($this->oid($table, FALSE, FALSE), $schema))->fetchField();
    }
    else {
      if (isset($column)) {
        return $this->connection->query('select comments from user_col_comments where column_name = ? and table_name = ?', array($this->oid($column, FALSE, FALSE), $this->oid($table, FALSE, FALSE)))->fetchField();
      }
      return $this->connection->query('select comments from user_tab_comments where table_name = ?', array($this->oid($table, FALSE, FALSE)))->fetchField();
    }
  }

  public static function tableSchema($table) {
    $exp = explode(".", $table);

    if (count($exp) > 1) {
      return strtoupper(str_replace('"', '', $exp[0]));
    }
    else {
      return FALSE;
    }
  }

  public function tableExists($table) {
    $schema = $this->tableSchema($this->connection->prefixTables('{' . strtoupper($table) . '}', TRUE));

    if ($schema) {
      $retval = $this->connection->query("SELECT 1 FROM all_tables WHERE temporary= 'N' and table_name = ? and owner= ?", array($this->oid($table, FALSE, FALSE), $schema))->fetchField();
    }
    else {
      $retval = $this->connection->query("SELECT 1 FROM user_tables WHERE temporary= 'N' and table_name = ?", array($this->oid($table, FALSE, FALSE)))->fetchField();
    }

    if ($retval) {
      return TRUE;
    }
    else {
      return FALSE;
    }
  }

  public function fieldExists($table, $column) {
    $schema = $this->tableSchema($this->connection->prefixTables('{' . strtoupper($table) . '}', TRUE));

    if ($schema) {
      $retval = $this->connection->query("SELECT 1 FROM all_tab_columns WHERE column_name = ? and table_name = ? and owner= ?", array($this->oid($column, FALSE, FALSE), $this->oid($table, FALSE, FALSE), $schema))->fetchField();
    }
    else {
      $retval = $this->connection->query("SELECT 1 FROM user_tab_columns WHERE column_name= ? and table_name = ?", array($this->oid($column, FALSE, FALSE), $this->oid($table, FALSE, FALSE)))->fetchField();
    }

    if ($retval) {
      return TRUE;
    }
    else {
      return FALSE;
    }
  }

  public function findTables($table_expression) {
    $schema = $this->tableSchema($table_expression);
    $table_expression = str_replace('"' . $schema . '"."', '', $table_expression);
    $table_expression = str_replace('"', '', $table_expression);
    $res = $this->connection->query("SELECT '\"'||owner||'\".\"'||table_name||'\"' tab FROM all_tables WHERE owner= ? and table_name LIKE ?", array($schema, strtoupper($table_expression)))->fetchAllKeyed(0, 0);

    return $res;
  }

  private function failsafeDdl($ddl) {
    try {
      $this->connection->query($ddl);
    }
    catch (\Exception $e) {
     // Ignore.
    }
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

  private function cleanUpSchema($cache_table, $trigger_table = '') {
    if (!$trigger_table) {
      $trigger_table = $cache_table;
    }

    $this->resetLongIdentifiers();
    $this->removeTableInfoCache($cache_table);
    $this->rebuildDefaultsTrigger($trigger_table);
  }

}
