<?php
// $Id$

/**
 * @ingroup database
 * @{
 */

class InsertQuery_oracle extends InsertQuery {
	
  public function __construct($connection, $table, array $options = array()) {
    parent::__construct($connection, $table, $options);
    $this->queryOptions['table_name']= $table;
    //syslog(LOG_ERR,"InsertQuery_oracle");
  }


  public function execute() {

    $last_insert_id = 0;

    // Confirm that the user did not try to specify an identical
    //  field and default field.
    if (array_intersect($this->insertFields, $this->defaultFields)) {
      throw new PDOException('You may not specify the same field to have a value and a schema-default value.');
    }

    if (count($this->insertFields) + count($this->defaultFields) == 0) {
      return NULL;
    }

    // Don't execute query without values.
    if (!isset($this->insertValues[0]) && count($this->insertFields) > 0) {
      return NULL;
    }
    
    $info= $this->connection->schema()->getTableInfo($this->table);
    //syslog(LOG_ERR,"table: ".$this->table." sequence_name: ".$info->sequence_name);
    
    if (!empty($info->sequence_name)) {
      $this->queryOptions["sequence_name"] = $info->sequence_name;
      $this->queryOptions['return'] = Database::RETURN_INSERT_ID;
    }
    
    
    $transaction = $this->connection->startTransaction();
    $stmt = $this->connection->prepareQuery((string)$this)->getStatement();
    $last_insert_id= 0;
    
    foreach ($this->insertValues as &$insert_values)
    {
        $max_placeholder = 0;
        
    	foreach ($this->insertFields as $idx => $field)
    	{
	        if (strlen($insert_values[$idx])>ORACLE_MAX_VARCHAR2_LENGTH)
	            $stmt->bindParam(':db_insert_placeholder_' . $max_placeholder++, $this->connection->writeBlob($insert_values[$idx]));
	        else
	            $stmt->bindParam(':db_insert_placeholder_'. $max_placeholder++, DatabaseConnection_oracle::nr($insert_values[$idx]));
        }
      
        $last_insert_id = $this->connection->query($stmt,  array(), $this->queryOptions);
    }
    
    if (count($this->insertValues)==0) // all default query
        $last_insert_id = $this->connection->query($stmt,  array(), $this->queryOptions);
    
    //syslog(LOG_ERR,"inserted id: ".print_r($last_insert_id,true));
    
    // Re-initialize the values array so that we can re-use this query.
    $this->insertValues = array();

    // Transaction commits here where $transaction looses scope.

    return $last_insert_id;
    
  }

  public function __toString() {

    $info= $this->connection->schema()->getTableInfo($this->table);

    //syslog(LOG_ERR,"info: ".print_r($info,true));
    
  	// Default fields are always placed first for consistency.
    $insert_fields = array_merge($this->defaultFields, $this->insertFields);

    $query = "INSERT INTO {" . $this->table . '} (' . implode(', ', $insert_fields) . ') VALUES ';

    $max_placeholder = 0;
    $values = array();
    
    if (count($this->insertValues))
    {
      	$placeholders = array();
        $placeholders = array_pad($placeholders, count($this->defaultFields), 'default');
        $i= 0;
        foreach ($this->insertFields as $key => $value) 
           $placeholders[] = ':db_insert_placeholder_' . $i++;
        
      
        $values= '(' . implode(', ', $placeholders) . ')';
        
    }
    else
    {
	    if (count($this->defaultFields)>0)
	    {
	        // If there are no values, then this is a default-only query. We still need to handle that.
	        $placeholders = array_fill(0, count($this->defaultFields), 'default');
	        $values = '(' . implode(', ', $placeholders) .')';
	    }
	    else
	      $values = '()'; // meaningless query that will not be executed
    }

    $query .= $values;
    
    //syslog(LOG_ERR,"insert_string: ".$query." insertValues: ".print_r($this->insertValues,true));
    
    return $query;
  }
  
}


class UpdateQuery_oracle extends UpdateQuery {
  public function execute() {
    $max_placeholder = 0;
    $blobs = array();
    $blob_count = 0;

    // Because we filter $fields the same way here and in __toString(), the
    // placeholders will all match up properly.
    $stmt = $this->connection->prepareQuery((string)$this)->getStatement();

    // Fetch the list of blobs and sequences used on that table.
    $info= $this->connection->schema()->getTableInfo($this->table);

    // Expressions take priority over literal fields, so we process those first
    // and remove any literal fields that conflict.
    $fields = $this->fields;
    $expression_fields = array();
    foreach ($this->expressionFields as $field => $data) {
      if (!empty($data['arguments'])) {
        foreach ($data['arguments'] as $placeholder => $argument) {
          // We assume that an expression will never happen on a BLOB field,
          // which is a fairly safe assumption to make since in most cases
          // it would be an invalid query anyway.
          $stmt->bindParam($placeholder, $argument);
        }
      }
      unset($fields[$field]);
    }

    foreach ($fields as $field => &$value)
    {
      $placeholder = ':db_update_placeholder_' . ($max_placeholder++);

      if (strlen($value)>ORACLE_MAX_VARCHAR2_LENGTH)
        $stmt->bindParam($placeholder, $this->connection->writeBlob($value));
      else
        $stmt->bindParam($placeholder, DatabaseConnection_oracle::nr($value));
    }

    if (count($this->condition)) {
      $this->condition->compile($this->connection);

      $arguments = $this->condition->arguments();
      foreach ($arguments as $placeholder => &$value) {
        $stmt->bindParam($placeholder, $value);
      }
    }

    $options = $this->queryOptions;
    $options['already_prepared'] = TRUE;
    $this->connection->query($stmt, array(), $options);
    ////syslog(LOG_ERR,"updateFields: ".print_r($this->fields,true));
    
    return $stmt->rowCount();
  }
}


class TruncateQuery_oracle extends TruncateQuery {

  public function __toString() {
    return 'TRUNCATE TABLE {' . $this->connection->escapeTable($this->table) . '} ';
  }
}

/**
 * @} End of "ingroup database".
 */