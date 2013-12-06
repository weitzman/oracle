<?php

/**
 * @file
 * Definition of Drupal\Driver\Database\oracle\Statement
 */

namespace Drupal\Driver\Database\oracle;

use Drupal\Core\Database\Statement as CoreStatement;

class Statement extends CoreStatement implements \IteratorAggregate {
  public function execute($args = array(), $options = array()) {
    if (!is_array($args) || !count($args)) {
      $args = NULL;
    }
    return parent::execute($args, $options);
  }

  public function fetch($fetch_style = NULL, $cursor_orientation = \PDO::FETCH_ORI_NEXT, $cursor_offset = 0) {
    return $this->dbh->cleanupFetched(parent::fetch($fetch_style, $cursor_orientation, $cursor_offset));
  }

  public function fetchField($index = 0) {
    return $this->dbh->cleanupFetched(parent::fetchField($index));
  }

  public function fetchObject($class_name = "stdClass", $constructor_args = NULL) {
    return $this->dbh->cleanupFetched(parent::fetchObject($class_name, $constructor_args));
  }

  public function fetchAssoc() {
    return $this->dbh->cleanupFetched(parent::fetchAssoc());
  }

  public function fetchAll($fetch_style = NULL, $fetch_argument = NULL, $ctor_args = NULL) {
    return $this->dbh->cleanupFetched(parent::fetchAll($fetch_style));
  }

  public function fetchCol($index = 0) {
    return $this->dbh->cleanupFetched(parent::fetchAll(\PDO::FETCH_COLUMN, $index));
  }

  public function fetchColumn($column_number = 0) {
    return $this->dbh->cleanupFetched(parent::fetchColumn($column_number));
  }

  public function fetchAllKeyed($key_index = 0, $value_index = 1) {
    return $this->dbh->cleanupFetched(parent::fetchAllKeyed($key_index, $value_index));
  }

  public function fetchAllAssoc($key, $fetch_style = NULL) {
    return $this->dbh->cleanupFetched(parent::fetchAllAssoc($key, $fetch_style));
  }

  public function getIterator() {
    return new \ArrayIterator($this->fetchAll());
  }
}
