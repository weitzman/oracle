<?php

namespace Drupal\Driver\Database\oracle;

use Drupal\Core\Database;
use Drupal\Core\Database\Query\Insert as QueryInsert;

/**
 * Oracle implementation of \Drupal\Core\Database\Query\Insert.
 */
class Insert extends QueryInsert {

  /**
   * {@inheritdoc}
   */
  public function __construct($connection, $table, array $options = array()) {
    parent::__construct($connection, $table, $options);
    $this->queryOptions['table_name'] = $table;
  }

  /**
   * {@inheritdoc}
   */
  public function execute() {
    if (!$this->preExecute()) {
      return NULL;
    }

    $table_information = $this->connection->schema()->queryTableInformation($this->table);
    // Oracle requires the table name to be specified explicitly
    // when requesting the last insert ID, so we pass that in via
    // the options array.
    $options = $this->queryOptions;


    if (class_exists("Drupal\Core\Database", FALSE)) {
      if (!empty($table_information->sequences)) {
        $options['sequence_name'] = $table_information->sequences[0];
      }
      // If there are no sequences then we can't get a last insert id.
      elseif ($options['return'] == Database::RETURN_INSERT_ID) {
        $options['return'] = Database::RETURN_NULL;
      }
    }

    $stmt = $this->connection->prepareQuery((string) $this);

    if (!empty($this->fromQuery)) {
      foreach ($this->fromQuery->getArguments() as $key => $value) {
        $value = $this->connection->cleanupArgValue($value);
        $stmt->bindParam($key, $value);
      }
      // The SelectQuery may contain arguments, load and pass them through.
      return $this->connection->query($stmt, array(), $options);
    }

    $last_insert_id = 0;
    $transaction = $this->connection->startTransaction();

    try {
      if (empty($this->insertValues)) {
        $last_insert_id = $this->connection->query($stmt, array(), $options);
      }
      else {
        foreach ($this->insertValues as &$insert_values) {
          $max_placeholder = 0;
          $blobs = [];
          $blob_count = 0;
          foreach ($this->insertFields as $idx => $field) {
            $insert_values[$idx] = $this->connection->cleanupArgValue($insert_values[$idx]);

            if (isset($table_information->blob_fields[strtoupper($field)])) {
              $blobs[$blob_count] = fopen('php://memory', 'a');
              fwrite($blobs[$blob_count], $insert_values[$idx]);
              rewind($blobs[$blob_count]);
              $stmt->bindParam(':db_insert_placeholder_' . $max_placeholder++, $blobs[$blob_count], \PDO::PARAM_LOB);

              // Pre-increment is faster in PHP than increment.
              ++$blob_count;
            }
            /* CLOB support */
            elseif (isset($table_information->clob_fields[strtoupper($field)])) {
              $insert_values[$idx] = (string) $insert_values[$idx];
              $stmt->bindParam(':db_insert_placeholder_' . $max_placeholder++, $insert_values[$idx], \PDO::PARAM_STR, strlen($insert_values[$idx]));
            }
            else {
              $stmt->bindParam(':db_insert_placeholder_' . $max_placeholder++, $insert_values[$idx]);
            }
          }
          $last_insert_id = $this->connection->query($stmt, [], $options);
        }
      }
    }
    catch (\Exception $e) {
      // One of the INSERTs failed, rollback the whole batch.
      // Transaction already is rolled back in Connection:query().
      $transaction->rollback();

      // Rethrow the exception for the calling code.
      throw $e;
    }

    // Re-initialize the values array so that we can re-use this query.
    $this->insertValues = array();

    return $last_insert_id;
  }

  /**
   * {@inheritdoc}
   */
  public function __toString() {
    $table_information = $this->connection->schema()->queryTableInformation($this->table);

    // Default fields are always placed first for consistency.
    $insert_fields = array_merge($this->defaultFields, $this->insertFields);

    if (!empty($this->fromQuery)) {
      $cols = implode(', ', $insert_fields);
      if (!empty($cols)) {
        return "INSERT INTO {" . $this->table . '} (' . $cols . ') ' . $this->fromQuery;
      }
      else {
        return "INSERT INTO {" . $this->table . '}  ' . $this->fromQuery;
      }
    }

    $query = "INSERT INTO {" . $this->table . '} (' . implode(', ', $insert_fields) . ') VALUES ';

    $max_placeholder = 0;
    $values = array();
    $blobs = [];

    if (count($this->insertValues)) {
      $placeholders = array();
      $placeholders = array_pad($placeholders, count($this->defaultFields), 'default');
      $i = 0;
      foreach ($this->insertFields as $idx => $field) {
        if (isset($table_information->blob_fields[strtoupper($field)])) {
          $blobs[$field] = ':db_insert_placeholder_' . $i++;
          $placeholders[] = 'EMPTY_BLOB()';
        }
        else {
          $placeholders[] = ':db_insert_placeholder_' . $i++;
        }
      }
      $values = '(' . implode(', ', $placeholders) . ')';
      if (!empty($blobs)) {
        $values .= ' RETURNING ' . implode(', ', array_keys($blobs)) . ' INTO ' . implode(', ', array_values($blobs));
      }
    }
    else {
      if (count($this->defaultFields) > 0) {
        // If there are no values, then this is a default-only query.
        // We still need to handle that.
        $placeholders = array_fill(0, count($this->defaultFields), 'default');
        $values = '(' . implode(', ', $placeholders) . ')';
      }
      else {
        // Meaningless query that will not be executed.
        $values = '()';
      }
    }

    $query .= $values;

    return $query;
  }

}
