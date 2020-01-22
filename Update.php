<?php

namespace Drupal\Driver\Database\oracle;

use Drupal\Core\Database\Database;
use Drupal\Core\Database\Driver\pgsql\Update as QueryUpdate;
use Drupal\Core\Database\Query\SelectInterface;

/**
 * Oracle implementation of \Drupal\Core\Database\Query\Update.
 */
class Update extends QueryUpdate {

  /**
   * {@inheritdoc}
   */
  public function execute() {
    // Check for blob fields as blobs need a transaction.
    $table_information = $this->connection->schema()->queryTableInformation($this->table);
    $has_blobs = FALSE;

    // Convert updates keys to upper case so that Postgres blob handling will
    // work and check for blob fields so that we can start a transaction if
    // needed.
    $fields = [];
    foreach ($this->fields as $field => $value) {
      $field = strtoupper($field);
      if (isset($table_information->blob_fields[$field])) {
        $has_blobs = TRUE;
      }

      $fields[$field] = $value;
    }
    $this->fields = $fields;

    if (!$has_blobs || $this->connection->inTransaction()) {
      return parent::execute();
    }

    // Blob writing only works while in a transaction.
    $transaction = $this->connection->startTransaction();
    try {
      return parent::execute();
    }
    catch (\Exception $e) {
      $transaction->rollback();
      throw $e;
    }
  }

  /**
   * {@inheritdoc}
   */
  public function __toString() {
    $query = parent::__toString();

    // Change the query for blobs to use the RETURNING SYNTAX.
    $table_information = $this->connection->schema()->queryTableInformation($this->table);

    $blobs = [];
    $i = 0;

    foreach ($this->fields as $field => $value) {
      if (isset($table_information->blob_fields[strtoupper($field)])) {
        $blobs[$this->connection->escapeField(strtoupper($field))] = ':db_update_placeholder_' . $i;
      }

      $i++;
    }

    // The syntax for updating a blob is:
    // UPDATE foo SET data = EMPTY_BLOB() WHERE bar = 42 RETURNING data INTO :d
    if (!empty($blobs)) {
      $query .= ' ';

      foreach ($blobs as $field => $placeholder) {
        // Find position of $placeholder in $query.
        $pos = strpos($query, $placeholder);
        if ($pos !== FALSE) {
          // Now replace only the first occurence of the $placeholder in the $query as
          // this avoids accidentally replacing an unrelated placeholder.
          $query = substr_replace($query, 'EMPTY_BLOB()', $pos, strlen($placeholder));
        }
      }

      $query .= 'RETURNING ' . implode(', ', array_keys($blobs)) . ' INTO ' . implode(', ', array_values($blobs));
    }

    return $query;
  }

}
