<?php

class Entity_One
extends Entity_Root
implements Target_Pgsqlable
{
    public function __construct()
    {
        parent::__construct('one');    
        
        $this[Entity_Root::VIEW_KEY] = new Entity_Columnset('key');
        $this[Entity_Root::VIEW_KEY]['one_id'] = new Entity_Column('one_id', Type::factory('uuid'));
        $this[Entity_Root::VIEW_KEY]->set_attribute(Entity_Root::REQUIRED, 'one_id');
    
        $this[Entity_Root::VIEW_TS] = new Entity_Columnset('timestamp');
        $this[Entity_Root::VIEW_TS]['modified_at'] = new Entity_Column('modified_at', Type::factory('date'));

        $this['api'] = new Entity_Columnset('api');
        $this['api']['one'] = new Entity_Column('one', Type::factory('string'));

        $this[Entity_Root::VIEW_SELECTOR] = new Entity_Columnset('selector');
        $this[Entity_Root::VIEW_SELECTOR]['one_id'] = new Entity_Column('one_id', Type::factory('uuid'));
        
        $this[Target_Pgsql::VIEW_IMMUTABLE] = new Entity_Columnset('pgsql_immutable');
        $this[Target_Pgsql::VIEW_IMMUTABLE]['one_id'] = new Entity_Column('one_id', Type::factory('uuid'));
                
        $this[Target_Pgsql::VIEW_MUTABLE] = new Entity_Columnset('pgsql_mutable');
        $this[Target_Pgsql::VIEW_MUTABLE]['one'] = new Entity_Column('one', Type::factory('string'));
        
        $target = new Target_Pgsql();
        $pgsql = new Target_Info_Pgsql();
        $pgsql->set_table('example_one');
        $this->set_target_info($target, $pgsql);
    }

}


class Entity_Two
extends Entity_Root
implements Target_Pgsqlable
{
    public function __construct()
    {
        parent::__construct('two');    
        
        $this[Entity_Root::VIEW_KEY] = new Entity_Columnset('key');
        $this[Entity_Root::VIEW_KEY]['two_id'] = new Entity_Column('two_id', Type::factory('uuid'));
        $this[Entity_Root::VIEW_KEY]->set_attribute(Entity_Root::REQUIRED, 'two_id');
    
        $this[Entity_Root::VIEW_TS] = new Entity_Columnset('timestamp');
        $this[Entity_Root::VIEW_TS]['modified_at'] = new Entity_Column('modified_at', Type::factory('date'));

        $this['api'] = new Entity_Columnset('api');
        $this['api']['two'] = new Entity_Column('two', Type::factory('string'));
        
        $this[Entity_Root::VIEW_SELECTOR] = new Entity_Columnset('selector');
        $this[Entity_Root::VIEW_SELECTOR]['two_id'] = new Entity_Column('two_id', Type::factory('uuid'));
        
        $this[Target_Pgsql::VIEW_IMMUTABLE] = new Entity_Columnset('pgsql_immutable');
        $this[Target_Pgsql::VIEW_IMMUTABLE]['two_id'] = new Entity_Column('two_id', Type::factory('uuid'));
                
        $this[Target_Pgsql::VIEW_MUTABLE] = new Entity_Columnset('pgsql_mutable');
        $this[Target_Pgsql::VIEW_MUTABLE]['two'] = new Entity_Column('two', Type::factory('string'));
        
        $target = new Target_Pgsql();
        $pgsql = new Target_Info_Pgsql();
        $pgsql->set_table('example_two');
        $this->set_target_info($target, $pgsql);
    }

}

class Mock_Database_Pgsql_Logger
extends Kohana_Database
{
    private $_expectations = array();
    
    public function __construct()
    {
    }
    
    public function connect()
    {
    }
    
    public function set_charset($charset)
    {
    }
    
    public function begin($mode = NULL)
    {
    }
    
    public function commit()
    {
    }
    
    public function rollback()
    {
    }
    
    public function list_tables($like = NULL)
    {
        return array();
    }

    public function list_columns($table, $like = NULL, $add_prefix = TRUE)
    {
        return array();
    }
    
    public function escape($value)
    {
        return "ESCAPED{" . $value . "}";
    }
    
    public function query($type, $sql, $as_object = false, array $params = NULL)
    {
        return new Mock_Query_Pgsql_Logger($this, $type, $sql, $as_object, $params);
    }
    
    public function expect($sql, $parameters, $result)
    {
        $this->_expectations[] = new Mock_Database_Expectation_Pgsql_Logger($sql, $parameters, $result);
    }
    
    public function actual($sql, $parameters)
    {
        if(empty($this->_expectations))
        {
            throw new Mock_Database_Exception_Pgsql_Logger("Was not expecting SQL, but got \"" . $sql . "\" " . var_export($parameters, TRUE) . ".");
        }
        
        $expectation = array_shift($this->_expectations);
        
        return $expectation->match($sql, $parameters);
        
        return $result;
    }
}

class Mock_Database_Exception_Pgsql_Logger extends Exception
{
}


class Mock_Database_Expectation_Pgsql_Logger
{
    private $_sql;
    private $_parameters;
    private $_result;
    
    public function __construct($sql, $parameters, $result)
    {
        $this->_sql = $sql;
        $this->_parameters = $parameters;
        $this->_result = $result;
    }
    
    public function match($sql, $parameters)
    {
        if($sql != $this->_sql)
        {
            throw new Mock_Database_Exception_Pgsql_Logger("Expected the SQL \"" . $this->_sql
                . "\", but got \"" . $sql . "\" " . var_export($parameters, TRUE) . ".");
        }
        
        return new Mock_Result_Pgsql_Logger($this->_result);
    }
}

class Mock_Query_Pgsql_Logger
{
    private $_db;
    private $_type;
    private $_sql;
    private $_as_object;
    private $_params;
    
    public function __construct($db, $type, $sql, $as_object, $params)
    {
        $this->_db = $db;
        $this->_sql = $sql;
    }
    
    public function parameters($params)
    {
        foreach($params as $name => $value)
        {
            $this->_params[$name] = $value;
        }
    }
    
    public function execute()
    {
        return $this->_db->actual($this->_sql, $this->_params);
    }
    
    public function as_array()
    {
        return $this->_db->actual($this->_sql, $this->_params)->as_array();
    }
}

class Mock_Result_Pgsql_Logger
{
    private $_result;
    
    public function __construct($result)
    {
        $this->_result = $result;
    }
    
    public function as_array()
    {
        return $this->_result;
    }
}


class PgsqlLoggerTest extends Unittest_TestCase
{
    public function testUpdateOneThenTwo()
    {
        $mock_database = new Mock_Database_Pgsql_Logger();
        $target = new Target_Pgsql($mock_database);
        
        $one = Entity_One::factory();
        $one['api']['one'] = 'One!';
        $selector = new Selector();
        $selector->exact('one_id', 'f0818405-a4c2-4f67-9a0e-d11a2f219d22');

        $mock_database->expect("UPDATE example_one SET \"one\" = :one WHERE ((one_id = 'f0818405-a4c2-4f67-9a0e-d11a2f219d22')) RETURNING one_id",
            array(
                'one' => 'One!',
                'one_id' => null,
                'modified_at' => null,
            ),
            array(
                array(
                    'one_id' => 'f0818405-a4c2-4f67-9a0e-d11a2f219d22',
                ),
            )
        );
        $mock_database->expect("SELECT one_id, modified_at, one, one_id FROM example_one WHERE ((one_id = 'f0818405-a4c2-4f67-9a0e-d11a2f219d22'))  ",
            array(),
            array()
        );
                
        $target->update($one, $selector);

        $two = Entity_Two::factory();
        $two['api']['two'] = 'Two!';
        $selector = new Selector();
        $selector->exact('two_id', 'db864da4-9a1e-44d0-976d-ec66c55e4f93');

        $mock_database->expect("UPDATE example_two SET \"two\" = :two WHERE ((two_id = 'db864da4-9a1e-44d0-976d-ec66c55e4f93')) RETURNING two_id",
            array(
                'two' => 'Two!',
                'two_id' => null,
                'modified_at' => null,
            ),
            array(
                array(
                    'two_id' => 'db864da4-9a1e-44d0-976d-ec66c55e4f93',
                ),
            )
        );
        $mock_database->expect("SELECT two_id, modified_at, two, two_id FROM example_two WHERE ((two_id = 'db864da4-9a1e-44d0-976d-ec66c55e4f93'))  ",
            array(),
            array()
        );

        $target->update($two, $selector);
        
        // The test is that we can get this far without error.
        // There was at one time a bug in the way Target_Pgsql used Logger which caused it to
        // break, so this test is for the regression.
    }
}
