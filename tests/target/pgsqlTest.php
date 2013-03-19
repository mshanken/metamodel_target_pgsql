<?php

class Entity_Example
extends Entity_Root
{
    public function __construct()
    {
        parent::__construct('example');    
        
        $this['key'] = new Entity_Columnset('key');
        $this['key']['primary_id'] = new Entity_Column('primary_id', Type::instance('uuid'), Entity_Columnset::REQUIRED);
    
        $this['timestamp'] = new Entity_Columnset('timestamp');
        $this['timestamp']['modified_at'] = new Entity_Column('modified_at', Type::instance('date'), Entity_Columnset::REQUIRED);

        $this['api'] = new Entity_Columnset('api');
        $this['api']['name'] = new Entity_Column('name', Type::instance('string'));
        
        $this[Target_Pgsql::VIEW_IMMUTABLE] = new Entity_Columnset('pgsql_mutable');
        $this[Target_Pgsql::VIEW_IMMUTABLE]['primary_id'] = new Entity_Column('primary_id', Type::instance('uuid'), Entity_Columnset::REQUIRED);
        $this[Target_Pgsql::VIEW_IMMUTABLE]['modified_at'] = new Entity_Column('modified_at', Type::instance('date'), Entity_Columnset::REQUIRED);
                
        $this[Target_Pgsql::VIEW_MUTABLE] = new Entity_Columnset('pgsql_mutable');
        $this[Target_Pgsql::VIEW_MUTABLE]['name'] = new Entity_Column('name', Type::instance('string'));
        
        $this->freeze();
        
        $pgsql = new Target_Info_Pgsql();
        $pgsql->set_table('example');
        $this->set_target_info($pgsql);
    }

}

class Mock_Database
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
        return new Mock_Query($this, $type, $sql, $as_object, $params);
    }
    
    public function expect($sql, $parameters, $result)
    {
        $this->_expectations[] = new Mock_Database_Expectation($sql, $parameters, $result);
    }
    
    public function actual($sql, $parameters)
    {
        if(empty($this->_expectations))
        {
            throw new Mock_Database_Exception("Was not expecting SQL, but got \"" . $sql . "\" " . var_export($parameters, TRUE) . ".");
        }
        
        $expectation = array_shift($this->_expectations);
        
        return $expectation->match($sql, $parameters);
        
        return $result;
    }
}

class Mock_Database_Exception extends Exception
{
}


class Mock_Database_Expectation
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
            throw new Mock_Database_Exception("Expected the SQL \"" . $this->_sql
                . "\", but got \"" . $sql . "\" " . var_export($parameters, TRUE) . ".");
        }
        
        return new Mock_Result($this->_result);
    }
}

class Mock_Query
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

class Mock_Result
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

class TargetTest extends Unittest_TestCase
{
    public function testSearch()
    {
        $mock_database = new Mock_Database();
        $target = new Target_Pgsql($mock_database);
        
        $entity = Entity_Example::factory();
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
        
        $rows = $target->select($entity, $selector);
        
        $this->assertInternalType('array', $rows);
        $this->assertEquals(2, count($rows));
        $this->assertInstanceOf('Entity_Row', $rows[0]);
        $this->assertEquals('171234e3-0993-4e9e-cc03-bb78c85a47b5', $rows[0]['key']['primary_id']);
        $this->assertEquals('2013-03-19 12:22:05', $rows[0]['timestamp']['modified_at']);
        $this->assertEquals('Row One', $rows[0]['api']['name']);
        $this->assertEquals('9a64d302-5513-446a-9895-ba5b26f0b2cf', $rows[1]['key']['primary_id']);
        $this->assertEquals('2013-03-19 12:23:14', $rows[1]['timestamp']['modified_at']);
        $this->assertEquals('Row Two', $rows[1]['api']['name']);
    }

    public function testDetails()
    {
        $mock_database = new Mock_Database();
        $target = new Target_Pgsql($mock_database);
        
        $entity = Entity_Example::factory();
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
        $this->assertEquals('171234e3-0993-4e9e-cc03-bb78c85a47b5', $rows[0]['key']['primary_id']);
        $this->assertEquals('2013-03-19 12:22:05', $rows[0]['timestamp']['modified_at']);
        $this->assertEquals('Row One', $rows[0]['api']['name']);
    }
    
    public function testPgsqlCreate()
    {
        $mock_database = new Mock_Database();
        $target = new Target_Pgsql($mock_database);
        
        $entity = Entity_Example::factory();
        $entity['timestamp']['modified_at'] = '2013-03-19 12:22:05';
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
        $this->assertEquals($entity['timestamp']['modified_at'],
                            $created['timestamp']['modified_at']);
        $this->assertEquals($entity['api']['name'],
                            $created['api']['name']);
        $this->assertInternalType('string', $created['key']['primary_id']);
    $this->assertRegExp('/^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$/', $created['key']['primary_id']);
        $this->assertEquals('e0209f19-d8c5-4ccf-945e-38700c375add', $created['key']['primary_id']);
    }
    
    public function testUpdate()
    {
        $mock_database = new Mock_Database();
        $target = new Target_Pgsql($mock_database);
        
        $entity = Entity_Example::factory();
        $entity['key']['primary_id'] = '171234e3-0993-4e9e-cc03-bb78c85a47b5';
        $entity['timestamp']['modified_at'] = '2013-03-19 12:22:05';
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
        $this->assertEquals('171234e3-0993-4e9e-cc03-bb78c85a47b5', $rows[0]['key']['primary_id']);
        $this->assertEquals('2013-03-19 12:22:05', $rows[0]['timestamp']['modified_at']);
        $this->assertEquals('Row One renamed', $rows[0]['api']['name']);
    }
    
    public function testDelete()
    {
        $mock_database = new Mock_Database();
        $target = new Target_Pgsql($mock_database);
        
        $entity = Entity_Example::factory();
        $entity['key']['primary_id'] = '171234e3-0993-4e9e-cc03-bb78c85a47b5';
        $entity['timestamp']['modified_at'] = '2013-03-19 12:22:05';
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
