<?php

class Entity_Example_Pgsql
extends Entity_Root
{
    public function __construct()
    {
        parent::__construct('example');    
        
        $this[Entity_Root::VIEW_KEY] = new Entity_Columnset('key');
        $this[Entity_Root::VIEW_KEY]['primary_id'] = new Entity_Column('primary_id', Type::factory('uuid'));
        $this[Entity_Root::VIEW_KEY]->set_attribute(Entity_Root::ATTR_REQUIRED, 'primary_id');
    
        $this[Entity_Root::VIEW_TS] = new Entity_Columnset('timestamp');
        $this[Entity_Root::VIEW_TS]['modified_at'] = new Entity_Column('modified_at', Type::factory('date_time'));
        $this[Entity_Root::VIEW_TS]->set_attribute(Entity_Root::ATTR_REQUIRED, 'modified_at');

        $this['api'] = new Entity_Columnset('api');
        $this['api']['name'] = new Entity_Column('name', Type::factory('string'));
        
        $this[Selector::VIEW_SELECTOR] = new Entity_Columnset('selector');
        $this[Selector::VIEW_SELECTOR]['primary_id'] = new Entity_Column('primary_id', Type::factory('uuid'));
        
        $this[Target_Pgsql::VIEW_IMMUTABLE] = new Entity_Columnset('pgsql_mutable');
        $this[Target_Pgsql::VIEW_IMMUTABLE]['primary_id'] = new Entity_Column('primary_id', Type::factory('uuid'), Entity_Root::ATTR_REQUIRED);
        $this[Target_Pgsql::VIEW_IMMUTABLE]['modified_at'] = new Entity_Column('modified_at', Type::factory('date_time'), Entity_Root::ATTR_REQUIRED);
                
        $this[Target_Pgsql::VIEW_MUTABLE] = new Entity_Columnset('pgsql_mutable');
        $this[Target_Pgsql::VIEW_MUTABLE]['name'] = new Entity_Column('name', Type::factory('string'));
        
        $this->freeze();
        
        $target = new Target_Pgsql();
        $pgsql = new Target_Info_Pgsql();
        $pgsql->set_table('example');
        $this->set_target_info($target, $pgsql);
    }

}

class Mock_Database_Pgsql
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
        return new Mock_Query_Pgsql($this, $type, $sql, $as_object, $params);
    }
    
    public function expect($sql, $parameters, $result)
    {
        $this->_expectations[] = new Mock_Database_Expectation_Pgsql($sql, $parameters, $result);
    }
    
    public function actual($sql, $parameters)
    {
        if(empty($this->_expectations))
        {
            throw new Mock_Database_Exception_Pgsql("Was not expecting SQL, but got \"" . $sql . "\" " . var_export($parameters, TRUE) . ".");
        }
        
        $expectation = array_shift($this->_expectations);
        
        return $expectation->match($sql, $parameters);
        
        return $result;
    }
}

class Mock_Database_Exception_Pgsql extends Exception
{
}


class Mock_Database_Expectation_Pgsql
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
            throw new Mock_Database_Exception_Pgsql("Expected the SQL \"" . $this->_sql
                . "\", but got \"" . $sql . "\" " . var_export($parameters, TRUE) . ".");
        }
        
        return new Mock_Result_Pgsql($this->_result);
    }
}

class Mock_Query_Pgsql
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

class Mock_Result_Pgsql
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

class PgsqlTest extends Unittest_TestCase
{
    public function testSearch()
    {
        $mock_database = new Mock_Database_Pgsql();
        $target = new Target_Pgsql($mock_database);
        
        $entity = Entity_Example_Pgsql::factory();
        $selector = new Selector();
        
        $mock_database->expect("SELECT primary_id, modified_at, name, primary_id, modified_at FROM example  ",
            array(),
            array(
                array(
                    'primary_id' => '171234e3-0993-4e9e-cc03-bb78c85a47b5',
                    'modified_at' => '2013-03-19 12:22:05',
                    'name' => 'Row One',
                ),
                array(
                    'primary_id' => '9a64d302-5513-446a-9895-ba5b26f0b2cf',
                    'modified_at' => '2013-03-19 12:23:14',
                    'name' => 'Row Two',
                ),
            )
        );
		//var_dump($selector);
        
        $rows = $target->select($entity, $selector);
        
        $this->assertInternalType('array', $rows);
        $this->assertEquals(2, count($rows));
        $this->assertInstanceOf('Entity_Row', $rows[0]);
        $this->assertEquals('171234e3-0993-4e9e-cc03-bb78c85a47b5', $rows[0][Entity_Root::VIEW_KEY]['primary_id']);
        $this->assertEquals('2013-03-19 12:22:05', $rows[0][Entity_Root::VIEW_TS]['modified_at']);
        $this->assertEquals('Row One', $rows[0]['api']['name']);
        $this->assertEquals('9a64d302-5513-446a-9895-ba5b26f0b2cf', $rows[1][Entity_Root::VIEW_KEY]['primary_id']);
        $this->assertEquals('2013-03-19 12:23:14', $rows[1][Entity_Root::VIEW_TS]['modified_at']);
        $this->assertEquals('Row Two', $rows[1]['api']['name']);
    }

    public function testDetails()
    {
        $mock_database = new Mock_Database_Pgsql();
        $target = new Target_Pgsql($mock_database);
        
        $entity = Entity_Example_Pgsql::factory();
        $selector = new Selector();
        $selector->exact('primary_id', '171234e3-0993-4e9e-cc03-bb78c85a47b5');
        
        $mock_database->expect("SELECT primary_id, modified_at, name, primary_id, modified_at FROM example WHERE ((primary_id = '171234e3-0993-4e9e-cc03-bb78c85a47b5'))  ",
            array(),
            array(
                array(
                    'primary_id' => '171234e3-0993-4e9e-cc03-bb78c85a47b5',
                    'modified_at' => '2013-03-19 12:22:05',
                    'name' => 'Row One',
                ),
            )
        );
        
        $rows = $target->select($entity, $selector);
        
        $this->assertInternalType('array', $rows);
        $this->assertEquals(1, count($rows));
        $this->assertInstanceOf('Entity_Row', $rows[0]);
        $this->assertEquals('171234e3-0993-4e9e-cc03-bb78c85a47b5', $rows[0][Entity_Root::VIEW_KEY]['primary_id']);
        $this->assertEquals('2013-03-19 12:22:05', $rows[0][Entity_Root::VIEW_TS]['modified_at']);
        $this->assertEquals('Row One', $rows[0]['api']['name']);
    }
    
    public function testPgsqlCreate()
    {
        $mock_database = new Mock_Database_Pgsql();
        $target = new Target_Pgsql($mock_database);
        
        $entity = Entity_Example_Pgsql::factory();
        $entity[Entity_Root::VIEW_TS]['modified_at'] = '2013-03-19 12:22:05';
        $entity['api']['name'] = 'An Entity';
        
        $mock_database->expect
            ("INSERT INTO example (name) VALUES (:name) RETURNING primary_id",
                array(
                    'name' => 'An Entity',
                ),
                array(
                    array(
                        'primary_id' => 'e0209f19-d8c5-4ccf-945e-38700c375add',
                    ),
                )
            );
        $mock_database->expect
            ("SELECT primary_id, modified_at, name, primary_id, modified_at FROM example WHERE ((primary_id = 'e0209f19-d8c5-4ccf-945e-38700c375add'))  ",
                array(
                ),
                array(
                    array(
                        'name' => 'An Entity',
                        'modified_at' => '2013-03-19 12:22:05',
                        'primary_id' => 'e0209f19-d8c5-4ccf-945e-38700c375add',  
                    ),
                )
            );
        $created = $target->create($entity);
        
        $this->assertInstanceOf('Entity_Row', $created);
        $this->assertEquals($entity[Entity_Root::VIEW_TS]['modified_at'],
                            $created[Entity_Root::VIEW_TS]['modified_at']);
        $this->assertEquals($entity['api']['name'],
                            $created['api']['name']);
        $this->assertInternalType('string', $created[Entity_Root::VIEW_KEY]['primary_id']);
    $this->assertRegExp('/^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$/', $created[Entity_Root::VIEW_KEY]['primary_id']);
        $this->assertEquals('e0209f19-d8c5-4ccf-945e-38700c375add', $created[Entity_Root::VIEW_KEY]['primary_id']);
    }
    
    public function testUpdate()
    {
        $mock_database = new Mock_Database_Pgsql();
        $target = new Target_Pgsql($mock_database);
        
        $entity = Entity_Example_Pgsql::factory();
        $entity[Entity_Root::VIEW_KEY]['primary_id'] = '171234e3-0993-4e9e-cc03-bb78c85a47b5';
        $entity[Entity_Root::VIEW_TS]['modified_at'] = '2013-03-19 12:22:05';
        $entity['api']['name'] = 'Row One renamed';
        
        $selector = new Selector();
        $selector->exact('primary_id', '171234e3-0993-4e9e-cc03-bb78c85a47b5');
        
        $mock_database->expect("UPDATE example SET \"name\" = :name WHERE ((primary_id = '171234e3-0993-4e9e-cc03-bb78c85a47b5')) RETURNING primary_id",
            array(
                'name' => 'Row One renamed',
                'primary_id' => '171234e3-0993-4e9e-cc03-bb78c85a47b5',
                'modified_at' => '2013-03-19 12:22:05'
            ),
            array(
                array(
                    'primary_id' => '171234e3-0993-4e9e-cc03-bb78c85a47b5',
                ),
            )
        );
        $mock_database->expect("SELECT primary_id, modified_at, name, primary_id, modified_at FROM example WHERE ((primary_id = '171234e3-0993-4e9e-cc03-bb78c85a47b5'))  ",
            array(),
            array(
                array(
                    'primary_id' => '171234e3-0993-4e9e-cc03-bb78c85a47b5',
                    'modified_at' => '2013-03-19 12:22:05',
                    'name' => 'Row One renamed',
                ),
            )
        );
        
        $rows = $target->update($entity, $selector);
        
        $this->assertInternalType('array', $rows);
        $this->assertEquals(1, count($rows));
        $this->assertInstanceOf('Entity_Row', $rows[0]);
        $this->assertEquals('171234e3-0993-4e9e-cc03-bb78c85a47b5', $rows[0][Entity_Root::VIEW_KEY]['primary_id']);
        $this->assertEquals('2013-03-19 12:22:05', $rows[0][Entity_Root::VIEW_TS]['modified_at']);
        $this->assertEquals('Row One renamed', $rows[0]['api']['name']);
    }
    
    public function testDelete()
    {
        $mock_database = new Mock_Database_Pgsql();
        $target = new Target_Pgsql($mock_database);
        
        $entity = Entity_Example_Pgsql::factory();
        $entity[Entity_Root::VIEW_KEY]['primary_id'] = '171234e3-0993-4e9e-cc03-bb78c85a47b5';
        $entity[Entity_Root::VIEW_TS]['modified_at'] = '2013-03-19 12:22:05';
        $entity['api']['name'] = 'Row One renamed';
        
        $selector = new Selector();
        $selector->exact('primary_id', '171234e3-0993-4e9e-cc03-bb78c85a47b5');
        
        $mock_database->expect("DELETE FROM example WHERE ((primary_id = '171234e3-0993-4e9e-cc03-bb78c85a47b5'))",
            array(),
            array()
        );
        
        $target->remove($entity, $selector);
    }
}
