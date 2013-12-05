<?php

/**
 * @file
 * Definition of Drupal\Driver\Database\oracle\Truncate
 */

namespace Drupal\Driver\Database\oracle;

use Drupal\Core\Database\Query\Truncate as QueryTruncate;

class Truncate extends QueryTruncate {
  public function __toString() {
    return 'TRUNCATE TABLE {' . $this->connection->escapeTable($this->table) . '} ';
  }
}
