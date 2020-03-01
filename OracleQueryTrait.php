<?php

namespace Drupal\Driver\Database\oracle;

use Drupal\Core\Database\Query\Condition;
use Drupal\Core\Database\Query\ConditionInterface;
use Drupal\Core\Database\Query\SelectInterface;

trait OracleQueryTrait {

  /**
   * @inheritDoc
   */
  public function preExecute($query = NULL) {
    // First, let modules alter the query.
    $return = parent::preExecute($query);

    // If no query object is passed in, use $this.
    if (!isset($query)) {
      $query = $this;
    }

    // Split large IN statements.
    $this->splitRecursive($query->conditions());

    return $return;
  }

  /**
   * Iterate over all conditions, even nested ones, and split large OR conditions.
   *
   * @param array $conditions
   */
  public function splitRecursive(&$conditions) {
    foreach ($conditions as &$condition) {
      if (isset($condition['field']) && $condition['field'] instanceof ConditionInterface) {
        $this->splitRecursive($condition['field']->conditions());
      }
      else {
        $this->splitIn($condition);
      }
    }
  }

  /**
   * Oracle can't parse 1000+ items in an IN clause.
   * Use multiple OR workaround https://stackoverflow.com/a/26223818/265501.
   *
   * @param $condition
   */
  public function splitIn(&$condition) {
    // The environment variable below is useful for testing/debugging.
    $max_size = getenv('ORACLE_IN_MAX_SIZE') ?: 999;
    if (isset($condition['operator']) && $condition['operator'] == 'IN' && count($condition['value']) > $max_size) {
      $chunks = array_chunk($condition['value'], $max_size);
      $group = $this->orConditionGroup();
      foreach ($chunks as $chunk) {
        $group->condition($condition['field'], $chunk, 'IN');
      }
      $condition = [
        'field' => $group,
        'value' => NULL,
        'operator' => '=',
      ];
    }
  }
}