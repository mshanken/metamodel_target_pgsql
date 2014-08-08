<?php //defined('SYSPATH') or die('No direct access allowed.');
/**
 * target/pgsql.php
 * 
 * @package Metamodel
 * @subpackage Target
 * @author dchan@mshanken.com
 *
 * TODO: Switch from encoding string to true parameterized queries
 *
 **/

Class Metamodel_Target_Pgsql 
extends Model_Database
implements Metamodel_Target
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
    }
    
    public function validate_entity(Resource_Record $record)
    {
        return $record instanceof Target_Pgsqlable;    
    }

    /**
     * implements selectable
     */
    public function select(Resource_Record $record, Selector $selector = null)
    {
        $record = clone $record;
        $this->select_deferred($record, $selector);
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
    public function select_count(Resource_Record $record, Selector $selector = null)
    {
        $record = clone $record;
        
        $sql = sprintf('SELECT count(*) AS count FROM %s', $record->get_root()->pgsql_view());            
        if (!is_null($selector)) 
        {
            if ($query = $selector->build_target_query($record, $this))
            {
                    if (!empty($query->where()))
                    {
                        $sql = sprintf('%s WHERE %s', $sql, $query->where());
                    }
            }
            // $sql = sprintf('%s %s %s', $sql, $selector->build_target_sort($record, $this), $selector->build_target_page($record, $this));
        }

        $results = $this->query(Database::SELECT, $sql)->execute()->as_array();
        return $results[0]['count'];
    }

    /**
     * implements selectable
     *
     * this function will attempt to create a new row in the postgresql db
     * from the contents of $record[pgsql_mutable + key + timestamp] 
     *
     */
    public function create(Resource_Record $record) 
    {   
        $record = clone $record;
        $record[Target_Pgsql::VIEW_MUTABLE]->validate();
        $problems = Logger::get('validation');
        if(!empty($problems))
        {
            throw new HTTP_Exception_400(var_export($problems, TRUE));
        }

        $returning_fields = array_merge(
            array_keys($record[Target_Pgsql::VIEW_MUTABLE]->me()->get_children())
            , array_keys($record[Resource_Representation::KEY]->me()->get_children())
            , array_keys($record[Resource_Representation::TS]->me()->get_children())
            , array_keys($record[Target_Pgsql::VIEW_IMMUTABLE]->me()->get_children())
            );

        $mutable_keys = array_keys($record[Target_Pgsql::VIEW_MUTABLE]->me()->get_children());

        if (!is_null($record->get_root()->pgsql_create_function())) 
        { 
            $sql = sprintf('SELECT %s FROM %s(:%s)',
                    implode(', ', array_keys($record[Resource_Representation::KEY]->me()->get_children()))
                    , $record->get_root()->pgsql_create_function()
                    , implode(', :', $mutable_keys)
                    );
            $query = $this->query(Database::SELECT, $sql);
            $query->parameters($this->PDO_params($record[Target_Pgsql::VIEW_MUTABLE]));
        }  
        else if (!is_null($record->get_root()->pgsql_view())) 
        {
            // INSERT 
            $key_fields = array_keys($record[Resource_Representation::KEY]->me()->get_children());
            $sql = sprintf('INSERT INTO %s (%s) VALUES (:%s) RETURNING %s'
                    , $record->get_root()->pgsql_table() 
                    , implode(', ', $mutable_keys)
                    , implode(', :', $mutable_keys)
                    , implode(', ', $key_fields)
            );
            $query = $this->query(Database::SELECT, $sql);
            $query->parameters($this->PDO_params($record[Target_Pgsql::VIEW_MUTABLE]));
            $results = $query->execute()->as_array();
            $row = array_shift($results);
            
            // SELECT FROM VIEW
            $where_clause = array();
            $row2 = array();
            foreach ($row as $key => $value)
            {
                if (!empty($value))
                {
                    $where_clause[] = sprintf(" (%s = :%s) ", $key, $key);
                    $row2[":$key"] = $value;
                }
            }

            $sql = sprintf('SELECT %s FROM %s WHERE %s'
                    , implode(', ', $returning_fields)
                    , $record->get_root()->pgsql_view()
                    , implode(' AND', $where_clause)
            );            
            $query = $this->query(Database::SELECT, $sql);
            $query->parameters($row2);

        }
        else if(count($returning_fields) > 0)
        {

            $sql = sprintf('INSERT INTO %s (%s) VALUES (:%s) RETURNING %s'
                , $record->get_root()->pgsql_table() 
                , implode(', ', $mutable_keys)
                , implode(', :', $mutable_keys) 
                , implode(', ', $returning_fields)
            );            
            $query = $this->query(Database::SELECT, $sql);
            $query->parameters($this->PDO_params($record[Target_Pgsql::VIEW_MUTABLE]));
        }
        else
        {
            $sql = sprintf('INSERT INTO %s (%s) VALUES (:%s)'
                , $record->get_root()->pgsql_table() 
                , implode(', ', $mutable_keys)
                , implode(', :', $mutable_keys) 
            );
            $query = $this->query(Database::SELECT, $sql);
            $query->parameters($this->PDO_params($record[Target_Pgsql::VIEW_MUTABLE]));
        }

        try 
        {
            $results = $query->execute()->as_array();
            $row = array_shift($results);
            $row = $this->decode($row);

            $record = $this->row_to_entity($record, $row);

        } catch (Kohana_Database_Exception $e) {
            $this->handle_exception($e);
        }

        Logger::reset('validation');
        return $record;    
    }

    /**
     * implements selectable
     *
     * if the view has no key/timestamp we add them to the end for updateFunctions
     *
     */
    public function update(Resource_Record $record, Selector $selector)
    {
        $record = clone $record;
        $record[Target_Pgsql::VIEW_MUTABLE]->validate();
        $problems = Logger::get('validation');
        $query = array();
        
        if(!empty($problems))
        {
            throw new HTTP_Exception_400(var_export($problems, TRUE));
        }

        $returning_fields = array_merge(
            array_keys($record[Target_Pgsql::VIEW_IMMUTABLE]->me()->get_children())
            , array_keys($record[Target_Pgsql::VIEW_MUTABLE]->me()->get_children())
            , array_keys($record[Resource_Representation::KEY]->me()->get_children())
            , array_keys($record[Resource_Representation::TS]->me()->get_children())
        );

        if (!is_null($record->get_root()->pgsql_update_function())) 
        { 
            $sp_parameter_fields = array_merge(
                array_keys($record[Resource_Representation::KEY]->me()->get_children())
                , array_keys($record[Resource_Representation::TS]->me()->get_children())
                , array_keys($record[Target_Pgsql::VIEW_MUTABLE]->me()->get_children())
            );
            $sql = sprintf('SELECT %s FROM %s(:%s)'
                , implode(', ', array_keys($record[Resource_Representation::KEY]->me()->get_children()))
                , $record->get_root()->pgsql_update_function()
                , implode(', :', $sp_parameter_fields)
            );
            $query = $this->query(Database::SELECT, $sql);
            $query->parameters($this->PDO_params($record[Target_Pgsql::VIEW_MUTABLE]));
            $query->parameters($this->PDO_params($record[Target_Pgsql::VIEW_IMMUTABLE]));
            $query->parameters($this->PDO_params($record[Resource_Representation::KEY]));
            $query->parameters($this->PDO_params($record[Resource_Representation::TS]));

        } 
        else if (!is_null($record->get_root()->pgsql_view())) 
        {
            // INSERT 
            $query = $selector->build_target_query($record, $this, $query);
            $where = $query['WHERE_CLAUSE'];
            
            $key_fields = array_keys($record[Resource_Representation::KEY]->me()->get_children());
                
            $sql = sprintf('UPDATE %s SET %s WHERE %s RETURNING %s' 
                , $record->get_root()->pgsql_table()
                , implode(', ', array_map(
                    function($abc) {return sprintf('"%s" = :%s', $abc, $abc);}
                    , array_keys($record[Target_Pgsql::VIEW_MUTABLE]->me()->get_children())
                ))
                , implode(', ', $where)
                , implode(', ', $key_fields)
            );

            $query = $this->query(Database::SELECT, $sql);
            $query->parameters($this->PDO_params($record[Target_Pgsql::VIEW_MUTABLE]));
            $query->parameters($this->PDO_params($record[Target_Pgsql::VIEW_IMMUTABLE]));
            $query->parameters($this->PDO_params($record[Resource_Representation::KEY]));
            $query->parameters($this->PDO_params($record[Resource_Representation::TS]));
            
            $results = $query->execute()->as_array();
            $row = array_shift($results);
            
            // SELECT FROM VIEW
            $where_clause = array();
            $row2 = array();
            foreach ($row as $key => $value)
            {
                if (!empty($value))
                {
                    $where_clause[] = sprintf(" (%s = :%s) ", $key, $key);
                    $row2[":$key"] = $value;
                }
            }

            $sql = sprintf('SELECT %s FROM %s WHERE %s'
                    , implode(', ', $returning_fields)
                    , $record->get_root()->pgsql_view()
                    , implode(' AND', $where_clause)
            );            
            $query = $this->query(Database::SELECT, $sql);
            $query->parameters($this->PDO_params($record[Target_Pgsql::VIEW_MUTABLE]));
            $query->parameters($this->PDO_params($record[Target_Pgsql::VIEW_IMMUTABLE]));
            $query->parameters($this->PDO_params($record[Resource_Representation::KEY]));
            $query->parameters($row2);


        }
        else if(count($returning_fields) > 0)
        {
            $query = $selector->build_target_query($record, $this, $query);
            $where = $query['WHERE_CLAUSE'];
                
            $sql = sprintf('UPDATE %s SET %s WHERE %s RETURNING %s'
                , $record->get_root()->pgsql_table()
                , implode(', ', array_map(
                    function($abc) {return sprintf('"%s" = :%s', $abc, $abc);}
                    , array_keys($record[Target_Pgsql::VIEW_MUTABLE]->me()->get_children())
                ))
                , implode(', ', $where)
                , implode(', ', $returning_fields)
            );
            $query = $this->query(Database::SELECT, $sql);
            $query->parameters($this->PDO_params($record[Target_Pgsql::VIEW_MUTABLE]));
            $query->parameters($this->PDO_params($record[Target_Pgsql::VIEW_IMMUTABLE]));
            $query->parameters($this->PDO_params($record[Resource_Representation::KEY]));
            $query->parameters($this->PDO_params($record[Resource_Representation::TS]));

        }
        else
        {
            $query = $selector->build_target_query($record, $this, $query);
            $where = $query['WHERE_CLAUSE'];
                
            $sql = sprintf('UPDATE %s SET %s WHERE %s'
                , $record->get_root()->pgsql_table()
                , implode(', ', array_map(
                    function($a) {return sprintf('"%s" = :%s', $a, $a);}
                    , array_keys($record[Target_Pgsql::VIEW_MUTABLE]->me()->get_children())
                ))
                ,  implode(', ', $where)
            );
            $query = $this->query(Database::SELECT, $sql);
            $query->parameters($this->PDO_params($record[Target_Pgsql::VIEW_MUTABLE]));
            $query->parameters($this->PDO_params($record[Target_Pgsql::VIEW_IMMUTABLE]));
            $query->parameters($this->PDO_params($record[Resource_Representation::KEY]));
            $query->parameters($this->PDO_params($record[Resource_Representation::TS]));

        }


        $results = $query->execute()->as_array();

        try 
        {
            //TODO - This assumes our update has returned rows. We need to handle when 0 rows are affected
            $row = array_shift($results);
            $row = $this->decode($row);

        } catch (Kohana_Database_Exception $e) {
            $this->handle_exception($e);
//        } catch (Exception $e) {
//            throw new HTTP_Exception_500($sql . "\n0000\n " . var_export($row, true) . $e->getMessage() );
        }

        Logger::reset('validation');

        return array($this->row_to_entity($record, $row));
    }


    /**
     * implements selectable
     *
     * @returns number of deleted rows
     */
    public function remove(Resource_Record $record, Selector $selector)
    {
        $record = clone $record;
        $query = $selector->build_target_query($record, $this);
        $where = implode(', ', $query['WHERE_CLAUSE']);

        $sql = sprintf('DELETE FROM %s WHERE %s', $record->get_root()->pgsql_table(), $where);

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

    public function select_deferred(Resource_Record $record, Selector $selector = null)
    {
        $query = $selector->build_target_query($record, $this);
       
           Logger::log('default', 'QUERY HERE............................................................................');
           Logger::log('default', var_export($query,true));

             $returning_fields = array_merge(
            array_keys($record[Resource_Representation::KEY]->me()->get_children())
            , array_keys($record[Resource_Representation::TS]->me()->get_children())
            , array_keys($record[Target_Pgsql::VIEW_MUTABLE]->me()->get_children())
            , array_keys($record[Target_Pgsql::VIEW_IMMUTABLE]->me()->get_children())
            //, $query['SELECT']
        );
        
        $query->select($returning_fields);
        
        
        if (is_null($record->get_root()->pgsql_view())) {
            throw new HTTP_Exception_500('DEV ERROR, Target_Info has no view or table defined');
        }

        $sql = sprintf('SELECT %s FROM %s', implode(', ', $query->select()), $record->get_root()->pgsql_view()); 
       
        if(!empty($query->where()))
        {
             $sql = sprintf('%s WHERE %s', $sql, $query->where());
        }
        
        if ($query->sort())
        {
            $sql = sprintf('%s ORDER BY %s', $sql, $query->sort());
        }

        if ($query->page())
        {
            $sql =  sprintf('%s %s', $sql, $query->page());
        }

        $this->select_query = $query;
        $this->select_data = $this->query(Database::SELECT, $sql)->execute()->as_array();
        $this->select_index = 0;
        $this->select_entity = $record;
    }

    public function next_row() 
    {
        if ($this->select_index < count($this->select_data))
        {
            $row = $this->select_data[$this->select_index++];
            $row = $this->decode($row);
            $record = clone $this->select_entity;

            return $this->row_to_entity($record, $row);
        }
        return false;
    }

    private function row_to_entity($record, $row)
    {
        $record[Resource_Representation::KEY] = $row;
        $record[Resource_Representation::TS] = $row;
        $record[Target_Pgsql::VIEW_MUTABLE] = $row;
        $record[Target_Pgsql::VIEW_IMMUTABLE] = $row;
        $record[Target_Pgsql::VIEW_OPTIONAL] = $row;

        return $record;
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
            $allowed = array(
                    Selector::SEARCH,
                    Selector::EXACT,
                    Selector::RANGE_MAX,
                    Selector::RANGE_MIN,
                    Selector::RANGE,
                    Selector::ISNULL,
                    Selector::DIST_RADIUS,
                    Selector::NEARBY,
                    );
        } 
        else if ($type instanceof Type_Date)
        {
            $allowed = array(
                    Selector::EXACT,
                    Selector::RANGE_MAX,
                    Selector::RANGE_MIN,
                    Selector::RANGE,
                    );

        }
        else if ($type instanceof Type_Typeable)
        {
            $allowed = array(
                    Selector::SEARCH,
                    Selector::EXACT,
                    Selector::ISNULL,
                    Selector::DIST_RADIUS,
                    );
        } 
        
        if ($sortable)
        {
            $allowed[] = Selector::SORT;
        }

        return $allowed;
    }

    
    private function find_alias_value(Resource_Record $record, $entanglement_name)
    {

        foreach (array(Resource_Representation::KEY, Resource_Representation::TS, Target_Pgsql::VIEW_MUTABLE, Target_Pgsql::VIEW_IMMUTABLE, Target_Pgsql::VIEW_OPTIONAL) as $view)
        {
            foreach ($record[$view]->get_children() as $alias => $value)
            {
                if ($value->find_by_entanglement_name($entanglement_name))
                {
                    return array($alias, $value);
                }
            }
        }
        return array(null,null);
    }


    /**
     * visit_exact
     *
     * satisfy selector visitor interface
     *
     * @param mixed $record
     * @param mixed $column_storage_name
     * @param array $query
     * @access public
     * @return Target_Query
     */
    public function visit_exact(Resource_Record $record, $entanglement_name, Target_Query $query)
    {
        list($alias, $search_value) = $this->find_alias_value($record, $entanglement_name);
        if ($search_value instanceof Resource_Type_Number)
        {
            $query->criteria( sprintf("(%s = %s)", $alias, $search_value) );
        } 
        else if (!is_null($search_value)) 
        {
            $query->criteria( sprintf("(%s = '%s')", $alias, pg_escape_string($search_value)));
        }

        return $query;
    }

    /**
     * visit_search
     *
     * @param Resource_Record $record
     * @param mixed $entanglement_name
     * @param Target_Query $query
     * @access public
     * @return Target_Query
     */
    public function visit_search(Resource_Record $record, $entanglement_name, Target_Query $query)
    {
        list($alias, $search_value) = $this->find_alias_value($record, $entanglement_name);

        $words = explode(' ', $search_value);
        foreach ($words as $token)
        {
            // @TODO we should have a word-break char before the search token, but 
            // we must handle beginning of string cases...
            // $query['WHERE'][] = sprintf("(%s ILIKE ' %s%%')", $alias, pg_escape_string($token));

            $query->criteria( sprintf("(%s ILIKE '%%%s%%')", $alias, pg_escape_string($token)) );
        }
        return $query;
    }

    /**
     * visit_max
     *
     * @param Resource_Record $record
     * @param mixed $entanglement_name
     * @param Target_Query $query
     * @access public
     * @return Target_Query
     */
    public function visit_max(Resource_Record $record, $entanglement_name, Target_Query $query)
    {
        list($alias, $search_value) = $this->find_alias_value($record, $entanglement_name);

        if ($search_value instanceof Type_Number)
        {
            $query->criteria( sprintf("(%s <= %d)", $alias, $search_value) );
        }
        else 
        {
            // handles dates
            $query->criteria( sprintf("(%s <= '%s')", $alias, $search_value) );
        }
        return $query;
    }

    /**
     * visit_min
     *
     * @param Resource_Record $record
     * @param mixed $entanglement_name
     * @param Target_Query $query
     * @access public
     * @return Target_Query
     */
    public function visit_min(Resource_Record $record, $entanglement_name, Target_Query $query)
    {
        list($alias, $search_value) = $this->find_alias_value($record, $entanglement_name);
        if ($search_value instanceof Type_Number)
        {
            $query->criteria( sprintf("(%s >= %d)", $alias, $search_value) );
        }
        else
        {
            // handles dates
            $query->criteria( sprintf("(%s >= '%s')", $alias, $search_value) );
        }
        return $query;
    }

    /**
     * visit_min
     *
     * @param Resource_Record $record
     * @param mixed $entanglement_name
     * @param Target_Query $query
     * @access public
     * @return Target_Query
     */
    public function visit_range(Resource_Record $record, $entanglement_name, Target_Query $query)
    {
        list($alias, $search_value) = $this->find_alias_value($record, $entanglement_name);
        if ($search_value instanceof Type_Number)
        {
            $query->criteria( sprintf("(%s BETWEEN %d AND %d)", $alias, $search_value['min'], $search_value['max']) );
        }
        else
        {
            // handles dates
            $query->criteria( sprintf("(%s BETWEEN '%s' AND '%s')", $alias, $search_value['min'], $search_value['max']) );
        }

        return $query;
    }

    /**
     * visit_range
     *
     * The geocodes are coded with Spatial Reference system ID (SRID) = 4326 
     * The output of ST_Distance and ST_DWithin is in degrees, to convert the output in nomal distance
     * measurements, following conversion units are used
     * 1 degree = 111128 meters -- dist: displays the distance from the point in meters
     * 1 kilometer x 0.00899 = degrees -- used to feed the ST_DWithin function
     * 
     * 1 degree = 1 latitude = 69.047 statute miles = 60 nautical miles = 111.12 kilometers // http://www.dslreports.com/faq/14295
     *  
     * A nautical mile is 1,852 meters, or 1.852 kilometers. 
     * In the English measurement system, a nautical mile is 1.1508 miles, or 6,076 feet. // http://science.howstuffworks.com/innovation/science-questions/question79.htm
     *
     * There from above we can convert
     * 
     * 1 degree = 60 x 1.1508 miles = 69.048 miles
     * 1 Mile = 1/ 60 x 1.1508 = 0.01448
     * 
     * If you have to use meters/kilometers in the api use the following multipliers for distance
     * In ST_Distance function multiply with 111128 ---- which converts degrees output from ST_Distance to Kilometers
     * and in the params array, multiply the distance with 0.00899 which would convert the kilometers passed from external apps to degrees
     * 
     * Similarly, If you have to use miles in the api use the following multipliers for distance
     * In ST_Distance function multiply with 69.048 ---- which converts degrees output from ST_Distance to Kilometers
     * and in the params array, multiply the distance with 0.01448 which would convert the kilometers passed from external apps to degrees

     * @param Resource_Record $record
     * @param mixed $entanglement_name
     * @param Target_Query $query
     * @access public
     * @TODO Set the SRID (=4326) in config file, so we do not need to hard-code it
     * @return Target_Query
     */
    // public function visit_dist_radius(Entity_Columnset_Iterator $view, $alias, array $query, $long, $lat, $radius) 
    public function visit_dist_radius(Resource_Record $record, $entanglement_name, Target_Query $query, $long, $lat, $radius)
    {
        list($alias, $search_value) = $this->find_alias_value($record, $entanglement_name);


        $radius = $radius * .01448;  // converting radius passed in Mile into degrees as required by ST_DWithin function

        if (is_numeric($long) && is_numeric($lat) && is_numeric($radius))
        {

            $children = $view->me()->get_children();

            if ($children[$alias] instanceof Type_Geometry)
            {
                $query->criteria( sprintf("ST_DWithin(%s, ST_GeometryFromText('POINT(%f %f)',4326), %f)", $column_name, $long, $lat, $radius) );

            }
            elseif ($children[$alias] instanceof Type_Point) 
            {
                $query->criteria( sprintf("ST_DWithin(ST_GeometryFromText('POINT'||regexp_replace(%s::text, ',', ' ')::text, 4326)::geometry, ST_GeometryFromText('POINT(%f %f)',4326), %f)", $column_name, $long, $lat, $radius) );
            }
            else 
            {
                throw new Exception ('Wrong data type field paseed to the selector. Selector accepts only Geometry or Point fields ');
            }
            return $query;    
        }

    }



    /**
     * satisfy selector visitor interface
     *
     */
    public function sort_nearby($record, $column_storage_name, array $query, $long, $lat) 
    {
        // @TODO why is column name hard coded instead of being defined in a view_optional ?
        // since for geom column which is a geometry type field there is no type defined in metamodel
        // it would be nice to have the geometry type defined in metamodel in order to just pass geom in the selector
        // I am currently using latitude in the selector which is kind of silly, just because I can associate the visit to numeric data type

        $column_name = "geom";

        if (is_numeric($long) && is_numeric($lat))
        {
            // $query['SORT_BY'] = sprintf("ORDER BY %s <-> 'SRID=4326;POINT(%f %f)'::geometry,zip", $column_name, $long, $lat);
            $query['SORTS'][] = sprintf("%s <-> 'SRID=4326;POINT(%f %f)'::geometry", $column_storage_name, $long, $lat);;

            $query['SELECT'][] = sprintf("round ( cast(((ST_Distance( ST_GeometryFromText('POINT'||regexp_replace(%s::text, ',', ' ')::text, 4326)::geometry, ST_GeometryFromText('POINT(%f %f)',4326))) * 69.048) as numeric), 2)  as distance", $column_storage_name, $long, $lat );

        }
        // print_r($query);


        return $query;

    }


    /**
     * visit_dist_radius
     *
     * @param Resource_Record $record
     * @param mixed $entanglement_name
     * @param Target_Query $query
     * @access public
     * @return Target_Query
     */
    public function visit_isnull(Resource_Record $record, $entanglement_name, Target_Query $query)
    {
        list($alias, $search_value) = $this->find_alias_value($record, $entanglement_name);

        $query->criteria( sprintf("(%s IS NULL)", $alias) );
        return $query;
    }

    /**
     * satisfy selector visitor interface
     */
    public function visit_operator_and(Target_Query $query) 
    {
        if (!empty($query->criteria()))
        {
            $query->where( sprintf('(%s)', implode(') AND (', $query->criteria())) );
        }
        return $query;
    }

    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_operator_or(Target_Query $query) 
    {
        if (!empty($query->criteria()))
        {
            $query->where( sprintf('(%s)', implode(') OR (', $query->criteria())) );
        }
        return $query;
    }

    /**
     * satisfy selector visitor interface
     */
    public function visit_operator_not(Target_Query $query) 
    {
        $parts = $query->criteria();
        if (count($parts) != 1)
        {
            throw new Exception ('selector operation not cannot accept multiple parts');
        }

        $query->where(sprintf('NOT (%s)', end($parts)) );
        return $query;
    }

    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_sort(Resource_Record $record, $entanglement_name, array $info, Target_Query $query)
        // public function visit_sort($record, array $item, array $query) 
    {
        list($alias, $search_value) = $this->find_alias_value($record, $entanglement_name);
        list($direction, $coordinates) = $info;
        if(!empty($direction) && !is_array($coordinates))
        {
            $query->sort_criteria( sprintf('%s %s', $alias, $direction));
        }
        else
        {
            $long = $coordinates[0];
            $lat = $coordinates[1];

            $query = $this->sort_nearby($record, $alias, $query, $long, $lat);

        }
        return $query;
    }

    public function visit_page($limit, $offset = 0, Target_Query $query)
    {
        if (empty($limit)) return $query;
Logger::log('debug', 'LIMIT '.$limit);
        $query->page(sprintf('LIMIT %d OFFSET %d', $limit, $offset));
        return $query;
    }

    /**
     * Helper for the visit_*() interface that builds WHERE clauses out of selectors.
     * Responsible for looking up an actual column name as it is seen by Postgres.
     */
    public function lookup_entanglement_name($record, $column_storage_name)
    {
        foreach(array(Resource_Representation::KEY, Resource_Representation::TS, Target_Pgsql::VIEW_MUTABLE, Target_Pgsql::VIEW_IMMUTABLE,) as $view_name)
        {
            if ($alias = $record[$view_name]->lookup_entanglement_name($column_storage_name))
            {
                return array($view_name,$alias);
            }
        }

        throw new HTTP_Exception_400("Unknown column \"" . $column_storage_name . "\" in entity \"" . $record->get_root()->get_name() . "\".");
    }

    /**
     * choke point for overriding/caching and logging of queries
     *
     */
      
    private function query($mode, $sql)
    {
        error_log( $sql );
        //echo $sql;
        
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
        $children = $view->me()->get_children();

        $view->validate();

        $result = array();
        foreach($view as $name => $value)
        {
            // this does not recurse because we only encode Entity_Columnable/Traversable Objects
            if ($value instanceof Entity_Array_Simple) // was is_array($value)
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
                foreach ($value as $val2) 
                {
                    if(is_null($val2) || is_scalar($val2))
                    {
                        $tmp[] = $this->addslashes($val2);
                    }
                    else
                    {
                        $tmp[] = $this->addslashes($this->encode($val2));
                    }
                }
                if(count($tmp) == 0) $result[$name] = "{}";
                else $result[$name] = sprintf('{"%s"}', implode('","', $tmp));
            }
            else
            {
                $type = $children[$name];
                
                if ($type instanceof Type_Number) 
                {
                    $result[$name] = $value;
                }
                else if ($type instanceof Type_String) 
                {
                    $value = trim($value);
                    if (strlen($value) == 0)
                    {
                        $result[$name] = NULL; // a null char
                    }
                    else
                    {
                        $result[$name] = $value;
                    }
                }
                else if ($type instanceof Type_Boolean) 
                {
                    $result[$name] = $value ? 'true' : 'false';
                }
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
    private function handle_exception(Kohana_Database_Exception  $exception)
    {
        $matches = array();
        $message = $exception->getMessage();
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
    public function select_custom(Resource_Record $template_entity, $sql, array $params = array()) 
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
        foreach($results as $row) 
        {

            $row = $this->decode($row);

            $record = clone $template_entity;
            $record[Resource_Representation::KEY] = $row;
            $record[Resource_Representation::TS] = $row;
            $record[Target_Pgsql::VIEW_MUTABLE] = $row;
            $record[Target_Pgsql::VIEW_IMMUTABLE] = $row;

            $entities[] = $record;
        }

        return $entities;
    }

    public function debug_info()
    {
        return NULL;
    }

    public function is_selectable(Resource_Record $row, $entanglement_name, array $allowed)
    {
        foreach (array(Resource_Representation::KEY
                    , Resource_Representation::TS
                    , Target_Pgsql::VIEW_MUTABLE
                    , Target_Pgsql::VIEW_IMMUTABLE
                    , Target_Pgsql::VIEW_OPTIONAL) as $view)
        {
            if ($row->offsetExists($view) and $row[$view]->lookup_entanglement_name($entanglement_name) !== false)
            {
                return true;
            }
        }
        return false;
    }

    public function add_selectable(Entity_Store $record, Selector $selector)
    {
        return true;
    }

    public function lookup_entanglement_data(Entity_Columnset_Iterator $view, $alias, $default)
    {
        if (array_key_exists($alias, $view))
        {
            return $view[$alias];
        }
        return $default;
    }
}
