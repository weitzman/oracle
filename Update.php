<?php

namespace Drupal\Driver\Database\oracle;

use Drupal\Core\Database\Database;
use Drupal\Core\Database\Driver\pgsql\Update as QueryUpdate;
use Drupal\Core\Database\Query\SelectInterface;

/**
 * Oracle implementation of \Drupal\Core\Database\Query\Update.
 */
class Update extends QueryUpdate {

  public function execute() {
    // Convert TABLE keys to upper case.
    $fields = [];
    foreach ($this->fields as $field => $value) {
      $fields[strtoupper($field)] = $value;
    }

    $this->fields = $fields;

    parent::execute();
  }

}
