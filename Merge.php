<?php

namespace Drupal\Driver\Database\oracle;

use Drupal\Core\Database\Query\Merge as QueryMerge;

/**
 * Oracle implementation of \Drupal\Core\Database\Query\Merge.
 */
class Merge extends QueryMerge {

  /**
   * {@inheritdoc}
   */
  function execute() {
    // Workaround a core bug: A field set in an expression should not be set in
    // the update as well.
    if ($this->expressionFields) {
      foreach ($this->expressionFields as $field => $data) {
        unset($this->updateFields[$field]);
      }
    }

    return parent::execute();
  }
}
