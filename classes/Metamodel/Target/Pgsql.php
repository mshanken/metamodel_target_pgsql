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
                    function($abc) {return sprintf('"%s" = :%s', $abc, $abc);}
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
            //TODO - This assumes our update has returned rows. We need to handle when 0 rows are affected
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
       

        if (!is_null($selector)) 
        {
           
            if (empty($query['SORT_BY']))
            {
                $query = $selector->build_target_sort($entity, $this, $query);    
            }
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
              
           if(isset($query['WHERE_CLAUSE']))
           {
               if(is_array($query['WHERE_CLAUSE']))
               {
                    if ($where = implode(', ', $query['WHERE_CLAUSE']))
                    {
                  
                      $where = sprintf('WHERE %s', $where);
                  
                   }
               } 
           }
    
       
        }


             $returning_fields = array_merge(
            array_keys($entity[Entity_Root::VIEW_KEY]->get_children())
            , array_keys($entity[Entity_Root::VIEW_TS]->get_children())
            , array_keys($entity[Target_Pgsql::VIEW_MUTABLE]->get_children())
            , array_keys($entity[Target_Pgsql::VIEW_IMMUTABLE]->get_children())
            //, $query['SELECT']
        );
        
        if (!empty($query['SELECT']))
            $query['SELECT'] = array_merge( $returning_fields, $query['SELECT']);
        
        else 
            $query['SELECT'] = $returning_fields;
        
        
        if (is_null($info->get_view())) {

            throw new HTTP_Exception_500('DEV ERROR, Target_Info has no view or table defined');
        }
        $sql = sprintf('SELECT %s FROM %s', implode(', ', array_filter($query['SELECT'])), $info->get_view()); 
        
        if(!empty($where))
             $sql = sprintf('%s %s', $sql, $where);
         
          $sql = sprintf('%s %s %s', $sql, $sort_by, $page_is);

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

    /**
     * visit_exact
     *
     * satisfy selector visitor interface
     *
     * @param mixed $entity
     * @param mixed $column_storage_name
     * @param array $query
     * @access public
     * @return void
     */
    public function visit_exact(Entity_Columnset_Iterator $view, $alias, $search_value, array $query)
    {
        $children = $view->get_children();
        if ($children[$alias] instanceof Type_Number) 
        {
            $query['WHERE'][] = sprintf("(%s = %s)", $alias, $search_value);
        } else {
            $query['WHERE'][] = sprintf("(%s = '%s')", $alias, pg_escape_string($search_value));
        }

        return $query;
    }

    /**
     * satisfy selector visitor interface
     *
     */ 
    public function visit_search(Entity_Columnset_Iterator $view, $alias, $search_value, array $query)
    {
        $words = explode(' ', $search_value);
        foreach ($words as $token)
        {
            // @TODO we should have a word-break char before the search token, but 
            // we must handle beginning of string cases...
            // $query['WHERE'][] = sprintf("(%s ILIKE ' %s%%')", $alias, pg_escape_string($token));

            $query['WHERE'][] = sprintf("(%s ILIKE '%%%s%%')", $alias, pg_escape_string($token));
        }
        return $query;
    }

    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_max(Entity_Columnset_Iterator $view, $alias, $search_value, array $query)
    {
        $children = $view->get_children();
        if ($children[$alias] instanceof Type_Number)
        {
            $query['WHERE'][] = sprintf("(%s <= %d)", $alias, $search_value);
        }
        else
        {
            // handles dates
            $query['WHERE'][] = sprintf("(%s <= '%s')", $alias, $search_value);
        }
        return $query;
    }

    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_min(Entity_Columnset_Iterator $view, $alias, $search_value, array $query)
    {
        $children = $view->get_children();
        if ($children[$alias] instanceof Type_Number)
        {
            $query['WHERE'][] = sprintf("(%s >= %d)", $alias, $search_value);
        }
        else
        {
            // handles dates
            $query['WHERE'][] = sprintf("(%s >= '%s')", $alias, $search_value);
        }
        return $query;
    }

    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_range(Entity_Columnset_Iterator $view, $alias, array $search_value, array $query)
    {
        $children = $view->get_children();
        if ($children[$alias] instanceof Type_Number)
        {
            $query['WHERE'][] = sprintf("(%s BETWEEN %d AND %d)", $alias, $search_value['min'], $search_value['max']);
        }
        else
        {
            // handles dates
            $query['WHERE'][] = sprintf("(%s BETWEEN '%s' AND '%s')", $alias, $search_value['min'], $search_value['max']);
        }

        return $query;
    }

    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_dist_radius(Entity_Columnset_Iterator $view, $alias, array $query, $long, $lat, $radius) 
    {
        /*
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
             * 
             * 
             * */    
        
        // @@TODO Set the SRID (=4326) in config file, so we do not need to hard-code it
        $column_name = $alias;
        
        $radius = $radius * .01448;  // converting radius passed in Mile into degrees as required by ST_DWithin function
        
        if (is_numeric($long) && is_numeric($lat) && is_numeric($radius))
        {
        
                $children = $view->get_children();
                
                if ($children[$alias] instanceof Type_Geometry)
                {
                    $query['WHERE'][] = sprintf("ST_DWithin(%s, ST_GeometryFromText('POINT(%f %f)',4326), %f)", $column_name, $long, $lat, $radius);

                }
                elseif ($children[$alias] instanceof Type_Point) 
                {
                    $query['WHERE'][] = sprintf("ST_DWithin(ST_GeometryFromText('POINT'||regexp_replace(%s::text, ',', ' ')::text, 4326)::geometry, ST_GeometryFromText('POINT(%f %f)',4326), %f)", $column_name, $long, $lat, $radius);
               
                }
                else {
                    
                     throw new Exception ('Wrong data type field paseed to the selector. Selector accepts only Geometry or Point fields ');
                }
                 return $query;    
        }
         
    }



    /**
     * satisfy selector visitor interface
     *
     */
    public function sort_nearby($entity, $column_storage_name, array $query, $long, $lat) 
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
     * satisfy selector visitor interface
     *
     */
    public function visit_isnull(Entity_Columnset_Iterator $view, $alias, array $query) 
    {
        $query['WHERE'][] = sprintf("(%s IS NULL)", $alias);
        return $query;
    }

    /**
     * satisfy selector visitor interface
     * @TODO remove unused $entity param
     */
    public function visit_operator_and(array $query) 
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
     * @TODO remove unused $entity param
     *
     */
    public function visit_operator_or(array $query) 
    {
        $parts = array();
        if(!empty($query['WHERE'])) 
        {    
            $parts = $query['WHERE'];    
            $query['WHERE_CLAUSE'][] = sprintf('(%s)', implode(') OR (', $parts));
        }


        return $query;
    }

    /**
     * satisfy selector visitor interface
     * @TODO remove unused $entity param
     */
    public function visit_operator_not(array $query) 
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
            foreach($items as $current)
            {
                    
                    $alias = "";    
    
                    //$current = explode(', ', $current);
    
                    list($column_name, $direction, $coordinates) = $current;
    
                    // get name of the column name as seen by Target Pgsql
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
                    if(!empty($alias) && !is_array($current[2]))
                    {
                        $query['SORTS'][] = sprintf('%s %s'
                            , $alias
                            , ($direction == 'desc') ? 'DESC' : 'ASC'
                            );
                    }
                    elseif(!empty($alias) && is_array($current[2])) 
                    {
                        //this is a special case for k nearest neighbors search using postgis KNN index
                        
                        $long = $coordinates[0];
                        $lat = $coordinates[1];
                    
                        $query = $this->sort_nearby($entity, $alias, $query, $long, $lat);
                    
                    
                    }
                }
               
         
        return $query;
    }

    public function visit_page($limit, $offset = 0, array $query)
    {
        if (empty($limit)) return $query;
        $query['LIMIT'] = sprintf('LIMIT %d OFFSET %d', $limit, $offset);
        return $query;
    }

    /**
     * Helper for the visit_*() interface that builds WHERE clauses out of selectors.
     * Responsible for looking up an actual column name as it is seen by Postgres.
     */
    public function lookup_entanglement_name($entity, $column_storage_name)
    {
        foreach(array(Entity_Root::VIEW_KEY, Entity_Root::VIEW_TS, Target_Pgsql::VIEW_MUTABLE, Target_Pgsql::VIEW_IMMUTABLE,) as $view_name)
        {
            if ($alias = $entity[$view_name]->lookup_entanglement_name($column_storage_name))
            {
                return array($view_name,$alias);
            }
        }

        throw new HTTP_Exception_400("Unknown column \"" . $column_storage_name . "\" in entity \"" . $entity->get_root()->get_name() . "\".");
    }

    /**
     * choke point for overriding/caching and logging of queries
     *
     */
      
    private function query($mode, $sql)
    {
         //error_log( $sql );
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
        $children = $view->get_children();

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
            if ($row->offsetExists($view) and $row[$view]->lookup_entanglement_name($entanglement_name) !== false)
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

    public function lookup_entanglement_data(Entity_Columnset_Iterator $view, $alias, $default)
    {
        if (array_key_exists($alias, $view))
        {
            return $view[$alias];
        }
        return $default;
    }
}
