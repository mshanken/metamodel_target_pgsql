<?php //defined('SYSPATH') or die('No direct access allowed.');
/**
 * target/pgsql.php
 * 
 * @package Metamodel
 * @subpackage Target
 * @author dchan@mshanken.com
 *
 **/

Class Metamodel_Target_Pgsql 
extends Model_Database
implements Target_Selectable
{
    /**
     * Immutable columns are those which are not updatable by clients of this code.  Key and
     * timestamp columns are the best examples.
     */
    const VIEW_IMMUTABLE = 'pgsql_immutable';
    
    /**
     * Mutable columns are those which are updatable by clients of this code.  Most columns are
     * mutable.
     */
    const VIEW_MUTABLE = 'pgsql_mutable';
	
	/**
     * Optinal columns are those which are updatable by clients of this code.  
     * computed fields such as distance ( in case of locations api) is an example
     */

	const VIEW_OPTIONAL = 'pgsql_optional';
	
	
    private $select_data;
    private $select_index;
    private $select_entity;
    private $_debug_db;
    
    public function __construct($debug_db = null)
    {
        $this->_debug_db = $debug_db;
		$query = array();
    }
    
    public function validate_entity(Entity_Row $entity)
    {
        return $entity instanceof Target_Pgsqlable;    
    }

    /**
     * implements selectable
     */
    public function select(Entity_Row $entity, Selector $selector = null)
    {
        $entity = clone $entity;
        $this->select_deferred($entity, $selector);
        $output = array();
        while ($curr = $this->next_row())
        {
            $output[] = $curr;
        }
        return $output;
    }
    
    /**
     * implements selectable
     */
    public function select_count(Entity_Row $entity, Selector $selector = null)
    {
        $entity = clone $entity;
        $info = $entity->get_root()->get_target_info($this);
		$query = array();
		
        $sql = sprintf('SELECT count(*) AS count FROM %s', $info->get_view());            
        if (!is_null($selector)) 
        {
            if ($query = $selector->build_target_query($entity, $this, $query))
            {
                if(is_array($query['WHERE_CLAUSE']))
				{	
	                $where = implode(', ', $query['WHERE_CLAUSE']);	
					
	                if (!empty($where))
	                {
	                    $sql = sprintf('%s WHERE %s', $sql, $where);
	                }
					
				}
            }
            // $sql = sprintf('%s %s %s', $sql, $selector->build_target_sort($entity, $this), $selector->build_target_page($entity, $this));
        }

        $results = $this->query(Database::SELECT, $sql)->execute()->as_array();
        return $results[0]['count'];
    }

    /**
     * implements selectable
     *
     * this function will attempt to create a new row in the postgresql db
     * from the contents of $entity[pgsql_mutable + key + timestamp] 
     *
     */
    public function create(Entity_Row $entity) 
    {   
        $entity = clone $entity;
        $info = $entity->get_root()->get_target_info($this);
        $entity[Target_Pgsql::VIEW_MUTABLE]->validate();
        $problems = Logger::get('validation');
        if(!empty($problems))
        {
            throw new HTTP_Exception_400(var_export($problems, TRUE));
        }

        $returning_fields = array_merge(
            array_keys($entity[Target_Pgsql::VIEW_MUTABLE]->get_children())
            , array_keys($entity[Entity_Root::VIEW_KEY]->get_children())
            , array_keys($entity[Entity_Root::VIEW_TS]->get_children())
            , array_keys($entity[Target_Pgsql::VIEW_IMMUTABLE]->get_children())
        );

        $mutable_keys = array_keys($entity[Target_Pgsql::VIEW_MUTABLE]->get_children());

        if (!is_null($info->get_create_function())) 
        { 
            $sql = sprintf('SELECT %s FROM %s(:%s)',
                implode(', ', array_keys($entity[Entity_Root::VIEW_KEY]->get_children()))
                , $info->get_create_function()
                , implode(', :', $mutable_keys)
            );
        } 
        else if(count($entity[Entity_Root::VIEW_KEY]->get_children()) > 0)
        {
            $sql = sprintf('INSERT INTO %s (%s) VALUES (:%s) RETURNING %s'
                , $info->get_table() 
                , implode(', ', $mutable_keys)
                , implode(', :', $mutable_keys) 
                , implode(', ', array_keys($entity[Entity_Root::VIEW_KEY]->get_children()))
            );
            
        }
        else
        {
            $sql = sprintf('INSERT INTO %s (%s) VALUES (:%s)'
                , $info->get_table() 
                , implode(', ', $mutable_keys)
                , implode(', :', $mutable_keys) 
            );
            
        }
        $query = $this->query(Database::SELECT, $sql);
        $query->parameters($this->PDO_params($entity[Target_Pgsql::VIEW_MUTABLE]));

        try 
        {
            $results = $query->execute()->as_array();
            $row = array_shift($results);
            $row = $this->decode($row);

            $selector = new Selector();
            foreach ($row as $k => $v) {
                $selector->exact($k, $v);
            }
            $created = $this->select($entity, $selector);
            $entity = array_shift($created);

        } catch (Kohana_Database_Exception $e) {
            $this->handle_exception($e);
        }

        Logger::reset('validation');
        return $entity;    
    }

    /**
     * implements selectable
     *
     * if the view has no key/timestamp we add them to the end for updateFunctions
     *
     */
    public function update(Entity_Row $entity, Selector $selector)
    {
        $entity = clone $entity;
        $info = $entity->get_root()->get_target_info($this);
        $entity[Target_Pgsql::VIEW_MUTABLE]->validate();
        $problems = Logger::get('validation');
		$query = array();
		
        if(!empty($problems))
        {
            throw new HTTP_Exception_400(var_export($problems, TRUE));
        }

        $returning_fields = array_merge(
            array_keys($entity[Target_Pgsql::VIEW_MUTABLE]->get_children())
            , array_keys($entity[Entity_Root::VIEW_KEY]->get_children())
            , array_keys($entity[Entity_Root::VIEW_TS]->get_children())
            , array_keys($entity[Target_Pgsql::VIEW_IMMUTABLE]->get_children())
        );

        if (!is_null($info->get_update_function())) 
        { 
            $sp_parameter_fields = array_merge(
                array_keys($entity[Entity_Root::VIEW_KEY]->get_children())
                , array_keys($entity[Entity_Root::VIEW_TS]->get_children())
                , array_keys($entity[Target_Pgsql::VIEW_MUTABLE]->get_children())
            );
            $sql = sprintf('SELECT %s FROM %s(:%s)'
                , implode(', ', array_keys($entity[Entity_Root::VIEW_KEY]->get_children()))
                , $info->get_update_function()
                , implode(', :', $sp_parameter_fields)
            );
        } 
        else if(count($entity[Entity_Root::VIEW_KEY]->get_children()) > 0)
        {
            $query = $selector->build_target_query($entity, $this, $query);
			$where = $query['WHERE_CLAUSE'];
				
            $sql = sprintf('UPDATE %s SET %s WHERE %s RETURNING %s'
                , $info->get_table()
                , implode(', ', array_map(
                    function($a) {return sprintf('"%s" = :%s', $a, $a);}
                    , array_keys($entity[Target_Pgsql::VIEW_MUTABLE]->get_children())
                ))
                , implode(', ', $where)
                , implode(', ', array_keys($entity[Entity_Root::VIEW_KEY]->get_children()))
            );
        }
        else
        {
            $query = $selector->build_target_query($entity, $this, $query);
			$where = $query['WHERE_CLAUSE'];
				
            $sql = sprintf('UPDATE %s SET %s WHERE %s'
                , $info->get_table()
                , implode(', ', array_map(
                    function($a) {return sprintf('"%s" = :%s', $a, $a);}
                    , array_keys($entity[Target_Pgsql::VIEW_MUTABLE]->get_children())
                ))
                ,  implode(', ', $where)
            );
        }

        $query = $this->query(Database::SELECT, $sql);
        $query->parameters($this->PDO_params($entity[Target_Pgsql::VIEW_MUTABLE]));
        $query->parameters($this->PDO_params($entity[Target_Pgsql::VIEW_IMMUTABLE]));
        $query->parameters($this->PDO_params($entity[Entity_Root::VIEW_KEY]));
        $query->parameters($this->PDO_params($entity[Entity_Root::VIEW_TS]));
        $results = $query->execute()->as_array();

        try 
        {
            $row = array_shift($results);
            $row = $this->decode($row);

            $selector = new Selector();
            foreach ($row as $k => $v) {
                $selector->exact($k, $v);
            }
            $out = $this->select($entity, $selector);
        } catch (Kohana_Database_Exception $e) {
            $this->handle_exception($e);
        }

        Logger::reset('validation');
        return $out;
    }


    /**
     * implements selectable
     *
     * @returns number of deleted rows
     */
    public function remove(Entity_Row $entity, Selector $selector)
    {
        $entity = clone $entity;
		$query = array();
        $info = $entity->get_root()->get_target_info($this);
        $query = $selector->build_target_query($entity, $this, $query);
		$where = implode(', ', $query['WHERE_CLAUSE']);

        $sql = sprintf('DELETE FROM %s WHERE %s', $info->get_table(), $where);

        $query = $this->query(Database::DELETE, $sql);
        try 
        {
            return $query->execute();
        } catch (Kohana_Database_Exception $e) {
            $this->handle_exception($e);
        }
    }





    









    /**
     * take an array a return an array suitable for passing to PDO
     * array ('a' => 'b') outputs array(':a' => $this->encode($b));
     *
     * @TODO profile me, i suspect this is slow
     */
    protected function PDO_params(Entity_Columnset_Iterator $eview) 
    {
        $encoded = $this->encode($eview);
        
        $result = array();
        foreach($encoded as $name => $value)
        {
            $result[":" . $name] = $value;
        }
        
        return $result;
    }




    // select helper
    public function select_deferred(Entity_Row $entity, Selector $selector = null)
    {
        $info = $entity->get_root()->get_target_info($this);
		$query = array();
		$query = $selector->build_target_query($entity, $this, $query);
		

        $returning_fields = array_merge(
            array_keys($entity[Entity_Root::VIEW_KEY]->get_children())
            , array_keys($entity[Entity_Root::VIEW_TS]->get_children())
            , array_keys($entity[Target_Pgsql::VIEW_MUTABLE]->get_children())
            , array_keys($entity[Target_Pgsql::VIEW_IMMUTABLE]->get_children())
        );
        

		if(isset($query['SELECT'] ))
			$returning_fields[] = $query['SELECT'];
		
		
		
        if (is_null($info->get_view())) {

            throw new HTTP_Exception_500('DEV ERROR, Target_Info has no view or table defined');
        }
        $sql = sprintf('SELECT %s FROM %s', implode(', ', array_filter($returning_fields)), $info->get_view());            

	         

        if (!is_null($selector)) 
        {
            if(is_array($query['WHERE_CLAUSE']))
			{
	            if ($where = implode(', ', $query['WHERE_CLAUSE']))
	            {
	              
				  $sql = sprintf('%s WHERE %s', $sql, $where);
				  
				    /*if ('()' != $where)
	                {
	                    $sql = sprintf('%s WHERE %s', $sql, $where);
	                }
					*/
	            }
			}
			$query = $selector->build_target_sort($entity, $this, $query);
			$sort_by = '';
			if(isset($query['SORT_BY']))
			{
				$sort_by = $query['SORT_BY'];
				
			}
			 
			 
			$query = $selector->build_target_page($entity, $this, $query);	
			
			$page_is = '';
			if(isset($query['LIMIT']))
			{
				$page_is = $query['LIMIT'];
			}	
           // $sql = sprintf('%s %s %s', $sql, $selector->build_target_sort($entity, $this, $query), $selector->build_target_page($entity, $this));
       	 	$sql = sprintf('%s %s %s', $sql, $sort_by, $page_is);
       
       
        }
		
		//echo $sql;
		
		

		$this->select_query = $query;
        $this->select_data = $this->query(Database::SELECT, $sql)->execute()->as_array();
        $this->select_index = 0;
        $this->select_entity = $entity;
    }

    public function next_row() 
    {
        if ($this->select_index < count($this->select_data))
        {
            $row = $this->select_data[$this->select_index++];
            $row = $this->decode($row);
            $entity = clone $this->select_entity;
//            $info = $entity->get_root()->get_target_info($this);

            $entity[Entity_Root::VIEW_KEY] = $row;
            $entity[Entity_Root::VIEW_TS] = $row;
            $entity[Target_Pgsql::VIEW_MUTABLE] = $row;
            $entity[Target_Pgsql::VIEW_IMMUTABLE] = $row;
			$entity[Target_Pgsql::VIEW_OPTIONAL] = $row;
			
			
        	return $entity;
        }
        return false;
    }

    public function count_rows() 
    {
        return count($this->select_data) - $this->select_index - 1;
    }

    /**
     * Return an array of valid methods which can be performed on the given type,
     * as defined by constants in the Selector class.
     */
    public function visit_selector_security(Type_Typeable $type, $sortable) {
        if ($type instanceof Type_Number)
        {
            return array(
                Selector::SEARCH,
                Selector::EXACT,
                Selector::RANGE_MAX,
                Selector::RANGE_MIN,
                Selector::RANGE,
                Selector::ISNULL,
                Selector::SORT,
                Selector::DIST_RADIUS,
            );
        } 
        else if ($type instanceof Type_Date)
        {
            return array(
                Selector::EXACT,
                Selector::RANGE_MAX,
                Selector::RANGE_MIN,
                Selector::RANGE,
                Selector::SORT,
            );

        }
        else if ($type instanceof Type_Typeable)
        {
            return array(
                Selector::SEARCH,
                Selector::EXACT,
                Selector::ISNULL,
                Selector::SORT,
            );
        } 
    }

    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_exact($entity, $column_storage_name, $param, array $query)
    {
        $column_name = $this->visit_column_name($entity, $column_storage_name, $query);
        // @TODO type check should be against column-type not param-value-type
        if (is_numeric($param)) {
            //return sprintf("(%s = %s)", $column_name, $param);
			 $query['WHERE'][] = sprintf("(%s = %s)", $column_name, $param);
        } else {
            //return sprintf("(%s = '%s')", $column_name, pg_escape_string($param));
            $query['WHERE'][] = sprintf("(%s = '%s')", $column_name, pg_escape_string($param));
        }
		
		return $query;
    }
    
    /**
     * satisfy selector visitor interface
     *
     */ 
    public function visit_search($entity, $column_storage_name, array $query, $param) 
    {
        $column_name = $this->visit_column_name($entity, $column_storage_name, $query);
       // return sprintf("(%s ILIKE '%%%s%%')", $column_name, pg_escape_string($param));
        $query['WHERE'][] = sprintf("(%s ILIKE '%%%s%%')", $column_name, pg_escape_string($param));
        
		return $query;
    }

    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_max($entity, $column_storage_name, array $query, $param) 
    {
        $column_name = $this->visit_column_name($entity, $column_storage_name, $query);
        if (is_numeric($param))
          
			 $query['WHERE'][] = sprintf("(%s <= %d)", $column_name, $param);
        else
            // handles dates
            
       		$query['WHERE'][] = sprintf("(%s <= '%s')", $column_name, $param);
	   return $query;
    }

    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_min($entity, $column_storage_name, array $query, $param) 
    {
        $column_name = $this->visit_column_name($entity, $column_storage_name, $query);
        if (is_numeric($param))
            $query['WHERE'][] = sprintf("(%s >= %d)", $column_name, $param);
        else
            // handles dates
            $query['WHERE'][] = sprintf("(%s >= '%s')", $column_name, $param);
        
		return $query;
    }

    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_range($entity, $column_storage_name, array $query, $min, $max) 
    {
        $column_name = $this->visit_column_name($entity, $column_storage_name, $query);
        if (is_numeric($min) && is_numeric($max))
            $query['WHERE'][] = sprintf("(%s BETWEEN %d AND %d)", $column_name, $min, $max);
        else
            // handles dates
            $query['WHERE'][] = sprintf("(%s BETWEEN '%s' AND '%s')", $column_name, $min, $max);
        
		
        return $query;
    }

    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_dist_radius($entity, $column_storage_name, array $query, $long, $lat, $radius) 
    {
        $column_name = "geom";
		$radius = $radius * .01448;
        if (is_numeric($long) && is_numeric($lat) && is_numeric($radius))
		{
			 //return sprintf("ST_DWithin(%s, ST_GeometryFromText('POINT(%f %f)',4326), %f)", $column_name, $long, $lat, $radius);
			$query['WHERE'][] = sprintf("ST_DWithin(%s, ST_GeometryFromText('POINT(%f %f)',4326), %f)", $column_name, $long, $lat, $radius);
			
			// I want to get an additional computed field with the results
			// round ( cast(((ST_Distance(geom, ST_GeometryFromText('POINT(:latitude :longitude)',4326))) * 69.048) as numeric), 2) dist
			
			$query['SELECT'] = sprintf("round ( cast(((ST_Distance(%s, ST_GeometryFromText('POINT(%f %f)',4326))) * 69.048) as numeric), 2)  as distance", $column_name, $long, $lat );
			
			
			
		}
       
       return $query;

    }
	
	
    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_isnull($entity, $column_storage_name, array $query) 
    {
        $column_name = $this->visit_column_name($entity, $column_storage_name, $query);
        //return sprintf("(%s IS NULL)", $column_name);
        $query['WHERE'][] = sprintf("(%s IS NULL)", $column_name);
		
		return $query;
    }

    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_operator_and($entity, array $query) 
    {
       	
		
        $parts = array();
        if(!empty($query['WHERE'])) 
        {
	        $parts = $query['WHERE'];	
	        
	        $query['WHERE_CLAUSE'][] = sprintf('(%s)', implode(') AND (', $parts));
			
		}
		//print_r($query);
		
		
		return $query;
    }

    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_operator_or($entity, array $query) 
    {
        $parts = array();
        if(!empty($query['WHERE'])) 
        {	
        	$parts = $query['WHERE'];	
			//var_dump($parts);
			
			
		
        	$query['WHERE_CLAUSE'][] = sprintf('(%s)', implode(') OR (', $parts));
		}
		
		
		return $query;
    }

    /**
     * satisfy selector visitor interface
     */
    public function visit_operator_not($entity, array $query) 
    {
        //return sprintf('NOT (%s)', $part);
        if (count($query['WHERE']) > 1) throw new Exception ('selector operation not cannot accept multiple parts');
        
		$part = $query['WHERE'][0];
        $query['WHERE_CLAUSE'][] = sprintf('NOT (%s)', $part);
		
		return $query;
		
		
    }

    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_sort($entity, array $items, array $query) 
    {
       
        $sorts = array();
        $i = 0;
        foreach($items as $current)
        {
            $alias = "";	
			
			//$current = explode(', ', $current);
			
            list($column_name, $direction) = $current;
			
			
			$alias = $entity[Target_Pgsql::VIEW_MUTABLE]->lookup_entanglement_name($column_name);
			
			if(!$alias)	
			{
            	$alias = $entity[Target_Pgsql::VIEW_IMMUTABLE]->lookup_entanglement_name($column_name);
				
			}
			
			if(!$alias)
			{
				if(isset($entity[Target_Pgsql::VIEW_OPTIONAL]))
					$alias = $entity[Target_Pgsql::VIEW_OPTIONAL]->lookup_entanglement_name($column_name);
			}
			if(!empty($alias))
			{	
			 
	           /* $sorts[] = sprintf('%s %s'
	                , $alias
	                , ($direction == 'desc') ? 'DESC' : 'ASC'
	            );
				*/
				$query['SORTS'][] = sprintf('%s %s'
	                , $alias
	                , ($direction == 'desc') ? 'DESC' : 'ASC'
	            );
			}
        }
		

     		//   if (!empty($sorts)) return sprintf('ORDER BY %s', implode(',', $sorts));
     	
	     	if (!empty($query['SORTS'])) 
				$query['SORT_BY'] = sprintf('ORDER BY %s', implode(',', $query['SORTS']));
				
	        return $query;
    }

    public function visit_page($entity, $query, $limit, $offset = 0)
    {
       //print_r($limit);
	   
	   
	    if (empty($limit)) return '';
        //return sprintf('LIMIT %d OFFSET %d', $limit, $offset);
        
        $query['LIMIT'] = sprintf('LIMIT %d OFFSET %d', $limit, $offset);
		
		
		return $query;
    }
    
    /**
     * Helper for the visit_*() interface that builds WHERE clauses out of selectors.
     * Responsible for looking up an actual column name as it is seen by Postgres.
     */
    private function visit_column_name($entity, $column_storage_name, array $query)
    {
        foreach(array(Entity_Root::VIEW_KEY, 'timestamp', 'pgsql_immutable', 'pgsql_mutable') as $view_name)
        {
            $column_name = $entity[$view_name]->lookup_entanglement_name($column_storage_name);
            if($column_name) return $column_name;
        }
        
        throw new HTTP_Exception_400("Unknown column \"" . $column_storage_name . "\" in entity \"" . $entity->get_root()->get_name() . "\".");
    }

    /**
     * choke point for overriding/caching and logging of queries
     *
     */
    private function query($mode, $sql)
    {
        // error_log( $sql );
        if(!is_null($this->_debug_db))
        {
            return $this->_debug_db->query($mode, $sql);
        }
        else
        {
            return DB::query($mode, $sql);    
        }
    }





    /**
     * all necessary transforms to put a column into pgsql
     * complex data are flattened into pgsql arrays
     * @TODO migrate away from pgsql array to json format
     */
    public function encode(Entity_Structure $view)
    {
        $children = $view->get_children();
        
        $view->validate();
        
        $result = array();
        foreach($view as $name => $value)
        {
            $type = $children[$name];
            
            // override point allows type to hijack their encoding...
            if (method_exists($type, 'transform_target_pgsql'))
            {
                $result[$name] = $type->transform_target_pgsql($value);
            } 
            // this does not recurse because we only encode Entity_Columnable/Traversable Objects
            else if ($value instanceof Entity_Array_Simple) // was is_array($value)
            {
                $concatenated = '{';
                foreach($value as $index => $subvalue)
                {
                    if($index > 0)
                    {
                        $concatenated .= ',';
                    }
                    $concatenated .= $subvalue;
                }
                $concatenated .= '}';
                
                $result[$name] = $concatenated;
            }
            // flatten into an psql array
            else if ($value instanceof Traversable)
            {
                $tmp = array();
                foreach ($value as $k => $v) 
                {
                    if(is_string($v))
                    {
                        $tmp[] = $this->addslashes($v);
                    }
                    else
                    {
                        $tmp[] = $this->addslashes($this->encode($v));
                    }
                }
                if(count($tmp) == 0) $result[$name] = "{}";
                else $result[$name] = sprintf('{"%s"}', implode('","', $tmp));
            }
            else if ($type instanceof Type_Number) 
            {
                $result[$name] = $value;
            }
            else if ($type instanceof Type_String) 
            {
                $value = trim($value);
                if (empty($value))
                {
                    $result[$name] = NULL; // a null char
                    // $result[$name] = 'NULL'; // a literal
                }
                else
                {
                    $result[$name] = $value;
                    // $result[$name] = pg_escape_string($value);
                }
            }
            else if ($type instanceof Type_Boolean) 
            {
                // encode boolean to string value
                $result[$name] = $value ? 'true' : 'false';
            }

        }
        
        return $result;
    }
    
    function addslashes($data)
    {
        if(is_string($data))
        {
            return strtr($data, array('\'' => '\\\'', '"' => '\\"', '\\' => '\\\\'));
        }
        else if(is_array($data))
        {
            $result = array();
            foreach($data as $key => $value)
            {
                $result[$key] = $this->addslashes($value);
            }
            if(count($result) == 0) return "{}";
            else return sprintf('{"%s"}', implode('","', $result));
        }
        else
        {
            return $data;
        }
    }

    /**
     * @TODO migrate away from pgsql arrays to json format (with 9.2)
     * all necessary transforms to change a postgres field into
     * a columnable data value
     */
    public function decode($field) 
    {
        $array = null;
				
        if (is_array($field))
        {
            foreach ($field as $k=>$v) 
            {
                $array[$k] = $this->decode($v);
                $nextlevel = $this->decode($array[$k]);
                while ($array[$k] != $nextlevel)
                {
                    $array[$k] = $nextlevel;                    
                    $nextlevel = $this->decode($nextlevel);
                }
            }
        } 
        else if ($field === 't')
        {
            return true;
        }
        else if ($field === 'f') 
        {
            return false;
        }
        else
        {
            $array = Parse::pg_parse($field);
        }

        if (is_null($array)) return $field;


        return $array;
    }
    
    // help with pgsql exceptions
    private function handle_exception(Kohana_Database_Exception  $e)
    {
        $matches = array();
        $message = $e->getMessage();
        preg_match('/SQLSTATE\[(.+)]:/', $message, $matches);
        if(count($matches) > 0) {
            $sqlstate = $matches[1];
        } else {
            $sqlstate = 0;
        }

        switch ($sqlstate)
        {
            case 23503:
                # Foreign key violation: 7 ERROR:
                # insert or update on table "vintages" violates foreign key constraint "vintages_wine_id_fkey"
                if (preg_match('/DETAIL:\s+Key \(([^)]+)\)=\(([^)]+)\) is not present in table "([^"]+)"/m', $message, $matches))
                {
                    $message = sprintf('No such %s with (%s = %s) exists', $matches[3], $matches[1], $matches[2]);
                    throw new HTTP_Exception_404($message);
                }
                # Foreign key violation: 7 ERROR:
                # update or delete on table \"wineries\" violates foreign key constraint \"wines_winery_id_fkey\" on table \"wines\"
                # DETAIL:  Key (winery_id)=(877d2b0a-6afe-11e2-a083-005056900008) is still referenced from table \"wines\".
                else if (preg_match('/Foreign key violation/', $message, $matches))
                {
                    if(preg_match('/DETAIL:\s+Key \(([^)]+)\)=\([^)]+\) is still referenced from table "([^"]+)"/m', $message, $matches))
                    {
                        $message = sprintf('Cannot delete because it (whatever has a %s) still owns at least one %s.', $matches[1], $matches[2]);
                    }
                    throw new HTTP_Exception_409($message);
                }
                throw new HTTP_Exception_500($message);
                break;
            case 23505:
                throw new HTTP_Exception_409($message);
                break;
            default:
                throw new HTTP_Exception_500($message);
        }
    }


    /**
     * run some custom sql, return the result
     */
    public function select_custom(Entity_Row $template_entity, $sql, array $params = array()) 
    {
        $query = $this->query(Database::SELECT, $sql);
        $query->parameters($params);
        try 
        {
            $results = $query->execute()->as_array();
        } catch (Kohana_Database_Exception $e) {
            $this->handle_exception($e);
        }
        
        $entities = array();
        $info = $template_entity->get_root()->get_target_info($this);
		
        foreach($results as $row) 
        {
        	
        	$row = $this->decode($row);
			
            $entity = clone $template_entity;
            $entity[Entity_Root::VIEW_KEY] = $row;
            $entity[Entity_Root::VIEW_TS] = $row;
            $entity[Target_Pgsql::VIEW_MUTABLE] = $row;
            $entity[Target_Pgsql::VIEW_IMMUTABLE] = $row;
			
            $entities[] = $entity;
        }
		
        return $entities;
    }
    
    public function debug_info()
    {
        return NULL;
    }

    public function is_selectable(Entity_Row $row, $entanglement_name, array $allowed)
    {
        foreach (array(Entity_Root::VIEW_KEY
                    , Entity_Root::VIEW_TS
                    , Target_Pgsql::VIEW_MUTABLE
                    , Target_Pgsql::VIEW_IMMUTABLE
					, Target_Pgsql::VIEW_OPTIONAL) as $view)
        {
            if ($row[$view]->lookup_entanglement_name($entanglement_name) !== false)
            {
                return true;
            }
        }
        return false;
    }

    public function add_selectable(Entity_Store $entity, Selector $selector)
    {
        return true;
    }
}
