<?php

/**
 * @file
 * Definition of Drupal\Driver\Database\oracle\StatementOracle
 */

namespace Drupal\Driver\Database\oracle;

class StatementOracle extends \PDOStatement {

  /**
   * Reference to the database connection object for this statement.
   *
   * The name $dbh is inherited from \PDOStatement.
   *
   * @var \Drupal\Core\Database\Connection
   */
  public $dbh;

  protected function __construct(Connection $dbh) {
    $this->dbh = $dbh;
    $this->setFetchMode(\PDO::FETCH_OBJ);
  }

  public function execute($args = array(), $options = array()) {
    if (isset($options['fetch'])) {
      if (is_string($options['fetch'])) {
        // \PDO::FETCH_PROPS_LATE tells __construct() to run before properties
        // are added to the object.
        $this->setFetchMode(\PDO::FETCH_CLASS | \PDO::FETCH_PROPS_LATE, $options['fetch']);
      }
      else {
        $this->setFetchMode($options['fetch']);
      }
    }

    if (is_array($args) && count($args)) {
      $return = parent::execute($args);
    }
    else {
      $return = parent::execute();
    }

    return $return;
  }
}
