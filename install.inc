<?php
// $Id$

// Oracle specific install functions

class DatabaseTasks_oracle extends DatabaseTasks {
  protected $pdoDriver = 'oci';
  
  public function name() {
    return 'Oracle';
  }
  
  public function __construct() {
    $this->tasks = array_merge(array(array(
      'function' => 'initializeDatabase',
      'arguments' => array(),
    )),$this->tasks);
//    syslog(LOG_ERR,"CONSTRUCT");
  }  
  
  
  private function oracle_query($sql, $args=NULL) {
    return Database::getConnection('default')->oracleQuery($sql,$args);
  }
  
  /**
   * Make Oracle Drupal friendly.
   */  
  public function initializeDatabase() 
  {
  	
    try 
    {
      syslog(LOG_ERR,"current dir: ".getcwd()); 	
	  $this->create_failsafe_objects("includes/database/oracle/resources/table");
	  $this->create_failsafe_objects("includes/database/oracle/resources/index");
	  $this->create_failsafe_objects("includes/database/oracle/resources/sequence");
	  $this->create_objects("includes/database/oracle/resources/function");
	  $this->create_objects("includes/database/oracle/resources/procedure");
	  $this->create_sp_objects("includes/database/oracle/resources/type");
	  $this->create_sp_objects("includes/database/oracle/resources/package");
      $this->oracle_query("begin dbms_utility.compile_schema(user); end;");
   
  	  $this->pass(st('Oracle has initialized itself.'));

    }
    catch (Exception $e)
    {
      syslog(LOG_ERR,$e->getMessage());
      $this->fail(st('Drupal could not be correctly setup with the existing database. Revise any errors.'));
    }
  	
  }
  
  private function create_sp_objects($dir_path)
  {
	    $dir = opendir($dir_path);
	    
	    while($name = readdir($dir))
	    {
	         if (in_array($name,array('.','..','.DS_Store','CVS'))) continue;
	         if (is_dir($dir_path."/".$name))
	           $this->create_sp_object($dir_path."/".$name);
	    }
  }

  private function create_sp_object($dir_path)
  {
	    $dir = opendir($dir_path);
	    
	    $spec= "";
	    $body= "";
	    
	    while ($name = readdir($dir))
	    {
	         if (substr($name,-4)=='.pls')
	           $spec= $name;
	         else
	         if (substr($name,-4)=='.plb')
	           $body= $name;
	    }
	    
	    $this->create_object($dir_path."/".$spec);
	    
	    if ($body)
	         $this->create_object($dir_path."/".$body);
  }

  private function create_objects($dir_path)
  {
	    $dir = opendir($dir_path);
	    
	    while($name = readdir($dir))
	    {
	         if (in_array($name,array('.','..','.DS_Store','CVS'))) continue;
	         $this->create_object($dir_path."/".$name);
	    }
  }
  
  private function create_object($file_path)
  {
         syslog(LOG_ERR,"creating object: $file_path");
         
         try
         {
            $this->oracle_query($this->get_php_contents($file_path));
         }
         catch (Exception $ex)
         {
             syslog(LOG_ERR,"object $file_path created with errors");         
         }
  }

  private function create_failsafe_objects($dir_path)
  {
	    $dir = opendir($dir_path);
	    
	    while($name = readdir($dir))
	    {
	         if (in_array($name,array('.','..','.DS_Store','CVS'))) continue;
	         
	         syslog(LOG_ERR,"creating object: $dir_path/$name");
	         $this->failsafe_ddl($this->get_php_contents($dir_path."/".$name));
	    }
  }

  private function failsafe_ddl($ddl) 
  {
        $this->oracle_query("begin execute immediate '".str_replace("'","''",$ddl)."'; exception when others then null; end;");
  }

  private function get_php_contents($filename)
  {
	    if (is_file($filename))
	    {
	       ob_start();
	       require_once $filename;
	       $contents = ob_get_contents();
	       ob_end_clean();
	       return $contents;
	    }
	    else
	       syslog(LOG_ERR,"error: file ".$filename." does not exists");
	
	    return false;
  }
  

}

