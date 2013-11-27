<?php

/**
 * @file
 * Definition of Drupal\Core\Database\Driver\oracle\Statement
 */

namespace Drupal\Core\Database\Driver\oracle;

use Drupal\Core\Database\Statement as CoreStatement;

class Statement extends CoreStatement implements \IteratorAggregate {
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
  private function cleanupFetched($f) {
    if ($this->dbh->external) {
      return $f;
    }

    if (is_array($f)) {
      foreach ($f as $key => $value) {
        if ((string) $key == strtolower(ORACLE_ROWNUM_ALIAS)) {
          unset($f[$key]);
        }
        // Long identifier.
        elseif (Connection::isLongIdentifier($key)) {
          $f[$this->dbh->getLongIdentifiersHandler()->longIdentifierKey($key)] = $this->cleanupFetched($value);
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
          $f->{$this->dbh->getLongIdentifiersHandler()->longIdentifierKey($key)} = $this->cleanupFetched($value);
          unset($f->{$key});
        }
        else {
          $f->{$key} = $this->cleanupFetched($value);
        }
      }
    }
    else {
      $f = $this->dbh->cleanupFetchedValue($f);
    }

    return $f;
  }

  public function fetch($fetch_style = NULL, $cursor_orientation = \PDO::FETCH_ORI_NEXT, $cursor_offset = 0) {
    return $this->cleanupFetched(parent::fetch($fetch_style, $cursor_orientation, $cursor_offset));
  }

  public function fetchField($index = 0) {
    return $this->cleanupFetched(parent::fetchField($index));
  }

  public function fetchObject($class_name = "stdClass", $constructor_args = NULL) {
    return $this->cleanupFetched(parent::fetchObject($class_name, $constructor_args));
  }

  public function fetchAssoc() {
    return $this->cleanupFetched(parent::fetchAssoc());
  }

  public function fetchAll($fetch_style = NULL, $fetch_argument = NULL, $ctor_args = NULL) {
    return $this->cleanupFetched(parent::fetchAll($fetch_style));
  }

  public function fetchCol($index = 0) {
    return $this->cleanupFetched(parent::fetchAll(\PDO::FETCH_COLUMN, $index));
  }

  public function fetchColumn($column_number = 0) {
    return $this->cleanupFetched(parent::fetchColumn($column_number));
  }

  public function fetchAllKeyed($key_index = 0, $value_index = 1) {
    return $this->cleanupFetched(parent::fetchAllKeyed($key_index, $value_index));
  }

  public function fetchAllAssoc($key, $fetch_style = NULL) {
    return $this->cleanupFetched(parent::fetchAllAssoc($key, $fetch_style));
  }

  public function getIterator() {
    return new \ArrayIterator($this->fetchAll());
  }
}
