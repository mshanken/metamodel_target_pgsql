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

    private $select_data;
    private $select_index;
    private $select_entity;
    
    public static function get_key()
    {
        return "pgsql";
    }

    public function validate_entity(Entity_Row $entity)
    {
        return $entity instanceof Target_Pgsqlable;    
    }

    /**
     * implements selectable
     */
    public function select(Entity_Row $e, Selector $selector = null)
    {
        $this->select_deferred($e, $selector);
        $output = array();
        while ($curr = $this->next_row()) $output[] = $curr;
        return $output;
    }
    
    /**
     * implements selectable
     */
    public function select_count(Entity_Row $entity, Selector $selector = null)
    {
        $info = $entity->get_root()->get_target_info($this);

        $sql = sprintf('SELECT count(*) AS count FROM %s', $info->get_view());            

        if (!is_null($selector)) 
        {
            if ($where = $selector->build_target_query($entity, $this))
            {
                if ('()' != $where)
                {
                    $sql = sprintf('%s WHERE %s', $sql, $where);
                }
            }
            // $sql = sprintf('%s %s %s', $sql, $selector->build_target_sort($entity, $this), $selector->build_target_page($entity, $this));
        }

        $results = $this->_db->query(Database::SELECT, $sql)->as_array();
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
        $info = $entity->get_root()->get_target_info($this);
        $entity[Target_Pgsql::VIEW_MUTABLE]->validate();
        $problems = Logger::get('validation');
        if(!empty($problems))
        {
            throw new HTTP_Exception_400(var_export($problems, TRUE));
        }

        $returning_fields = array_merge(
            array_keys($entity[Target_Pgsql::VIEW_MUTABLE]->to_array())
            , array_keys($entity['key']->to_array())
            , array_keys($entity['timestamp']->to_array())
            , array_keys($entity[Target_Pgsql::VIEW_IMMUTABLE]->to_array())
        );

        $mutable_keys = array_keys($entity[Target_Pgsql::VIEW_MUTABLE]->to_array());

        if (!is_null($info->get_create_function())) 
        { 
            $sql = sprintf('SELECT %s FROM %s(:%s)',
                implode(', ', array_keys($entity['key']->to_array()))
                , $info->get_create_function()
                , implode(', :', $mutable_keys)
            );
        } 
        else
        {
            $sql = sprintf('INSERT INTO %s (%s) VALUES (:%s) RETURNING %s'
                , $info->get_table() 
                , implode(', ', $mutable_keys)
                , implode(', :', $mutable_keys) 
                , implode(', ', array_keys($entity['key']->to_array()))
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
        $info = $entity->get_root()->get_target_info($this);
        $entity[Target_Pgsql::VIEW_MUTABLE]->validate();
        $problems = Logger::get('validation');
        if(!empty($problems))
        {
            throw new HTTP_Exception_400(var_export($problems, TRUE));
        }

        $returning_fields = array_merge(
            array_keys($entity[Target_Pgsql::VIEW_MUTABLE]->to_array())
            , array_keys($entity['key']->to_array())
            , array_keys($entity['timestamp']->to_array())
            , array_keys($entity[Target_Pgsql::VIEW_IMMUTABLE]->to_array())
        );

        if (!is_null($info->get_update_function())) 
        { 
            $sp_parameter_fields = array_merge(
                array_keys($entity['key']->to_array())
                , array_keys($entity['timestamp']->to_array())
                , array_keys($entity[Target_Pgsql::VIEW_MUTABLE]->to_array())
            );
            $sql = sprintf('SELECT %s FROM %s(:%s)'
                , implode(', ', array_keys($entity['key']->to_array()))
                , $info->get_update_function()
                , implode(', :', $sp_parameter_fields)
            );
        } 
        else  
        {
            $sql = sprintf('UPDATE %s SET %s WHERE %s RETURNING %s'
                , $info->get_table()
                , implode(', ', array_map(
                    function($a) {return sprintf('"%s" = :%s', $a, $a);}
                    , array_keys($entity[Target_Pgsql::VIEW_MUTABLE]->to_array())
                ))
                , $selector->build_target_query($entity->get_root(), $this)
                , implode(', ', array_keys($entity['key']->to_array()))
            );
        }
        $query = $this->query(Database::SELECT, $sql);
        $query->parameters($this->PDO_params($entity[Target_Pgsql::VIEW_MUTABLE]));
        $query->parameters($this->PDO_params($entity[Target_Pgsql::VIEW_IMMUTABLE]));
        $query->parameters($this->PDO_params($entity['key']));
        $query->parameters($this->PDO_params($entity['timestamp']));

        try 
        {
            $subselectors = array();
            $results = $query->execute()->as_array();
            foreach ($results as $rownum=>$row)
            {
                $sub_selector = new Selector();
                $row = $this->decode($row);
                foreach ($row as $k => $v) {
                    $sub_selector->exact($k, $v);
                }
                if ($selector) 
                {
                    $selector = Selector::union(array($selector, $sub_selector));
                } else {
                    $selector = $sub_selector;
                }
            }
            $out = $this->select($entity, $selector);
        } catch (Kohana_Database_Exception $e) {
            $this->handle_exception($e);
        }

        return $out;
    }


    /**
     * implements selectable
     *
     * @returns number of deleted rows
     */
    public function remove(Entity_Row $e, Selector $selector)
    {
        $info = $e->get_root()->get_target_info($this);
        $where = $selector->build_target_query($e->get_root(), $this);

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
    protected function PDO_params(Entity_Columnset $eview) 
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

        $returning_fields = array_merge(
            array_keys($entity['key']->to_array())
            , array_keys($entity['timestamp']->to_array())
            , array_keys($entity[Target_Pgsql::VIEW_MUTABLE]->to_array())
            , array_keys($entity[Target_Pgsql::VIEW_IMMUTABLE]->to_array())
        );
        
        if (is_null($info->get_view())) {
            throw new HTTP_Exception_500('DEV ERROR, Target_Info has no view or table defined');
        }
        $sql = sprintf('SELECT %s FROM %s', implode(', ', $returning_fields), $info->get_view());            

        if (!is_null($selector)) 
        {
            if ($where = $selector->build_target_query($entity->get_root(), $this))
            {
                if ('()' != $where)
                {
                    $sql = sprintf('%s WHERE %s', $sql, $where);
                }
            }
            $sql = sprintf('%s %s %s', $sql, $selector->build_target_sort($entity->get_root(), $this), $selector->build_target_page($entity->get_root(), $this));
        }

        $this->select_data = $this->_db->query(Database::SELECT, $sql)->execute()->as_array();
        $this->select_index = 0;
        $this->select_entity = $entity->get_root();
    }

    public function next_row() 
    {
        if ($this->select_index < count($this->select_data))
        {
            $row = $this->select_data[$this->select_index++];
            $row = $this->decode($row);
            $entity = $this->select_entity->row();
            $info = $entity->get_root()->get_target_info($this);
            $entity['key'] = $row;
            $entity['timestamp'] = $row;
            $entity[Target_Pgsql::VIEW_MUTABLE] = $row;
            $entity[Target_Pgsql::VIEW_IMMUTABLE] = $row;

            return $entity;
        }
        return false;
    }

    public function count_rows() 
    {
        return count($this->select_data) - $this->select_index - 1;
    }

    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_exact($entity, $column_name, $param)
    {
        // @TODO type check should be against column-type not param-value-type
        if (is_numeric($param)) {
            return sprintf("(%s = %s)", $column_name, $param);
        } else {
            return sprintf("(%s = '%s')", $column_name, pg_escape_string($param));
        }
    }
    
    /**
     * satisfy selector visitor interface
     *
     */ 
    public function visit_search($entity, $column_name, $param) 
    {
        return sprintf("(%s ILIKE '%%%s%%')", $column_name, pg_escape_string($param));
        break;
    }

    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_max($entity, $column_name, $param) 
    {
        return sprintf("(%s <= %d)", $column_name, $param);
        break;
    }

    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_min($entity, $column_name, $param) 
    {
        return sprintf("(%s >= %d)", $column_name, $param);
        break;
    }

    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_range($entity, $column_name, $min, $max) 
    {
        return sprintf("(%s BETWEEN %d AND %d)", $column_name, $min, $max);
    }

    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_operator_and($entity, array $parts) 
    {
        return sprintf('(%s)', implode(') AND (', $parts));
    }

    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_operator_or($entity, array $parts) 
    {
        return sprintf('(%s)', implode(') OR (', $parts));
    }

    /**
     * satisfy selector visitor interface
     */
    public function visit_operator_not($entity, $part) 
    {
        return sprintf('NOT (%s)', $part);
    }


    public function visit_sort($entity, array $items) 
    {
        $tmp = array();
        foreach ($items as $item) 
        {
           $tmp[] = sprintf('%s %s', $item[0], $item[1]);
        }
        if (!empty($tmp)) return sprintf('ORDER BY %s', implode(',', $tmp));
        return  '';
    }

    public function visit_page($entity, $limit, $offset = 0)
    {
        if (empty($limit)) return '';
        return sprintf('LIMIT %d OFFSET %d', $limit, $offset);
    }

    /**
     * choke point for overriding/caching and logging of queries
     *
     */
    private function query($mode, $sql)
    {
        error_log( $sql );
        return $this->_db->query($mode, $sql);    
    }





    /**
     * all necessary transforms to put a column into pgsql
     * complex data are flattened into pgsql arrays
     * @TODO migrate away from pgsql array to json format
     */
    public function encode(Entity_Structure $view)
    {
        $result = array();
        foreach($view as $name => $type)
        {
            $value = $view->offsetGet($name);
            
            // redundant in a recursive function ?
            if (!$type->validate($value, false)) throw new Exception(sprintf('your data, %s, for column, %s, is invalid', $value, get_class($type)));

            // override point allows type to hijack their encoding...
            if (method_exists($type, 'transform_target_pgsql'))
            {
                $result[$name] = $type->transform_target_pgsql($value);
            } 
            // flatten into an psql array
            else if ($type instanceof Traversable)
            {
                $tmp = array();
                foreach ($type as $k => $v) 
                {
                   // $tmp[] = trim($this->encode($v),'"');
                   $tmp[] = addslashes($this->encode($v));
                }
                if(count($tmp) == 0) $result[$name] = "{}";
                else $result[$name] = sprintf('{"%s"}', implode('","', $tmp));
            }
            // this does not recurse because we only encode Entity_Columnable/Traversable Objects
            else if (is_array($value))
            {
                $result[$name] = sprintf('{%s}', implode(',', $value));
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
        }
        
        return $result;
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
        else 
        {
            $array = Parse::pg_parse($field);
        }
        if (is_null($array)) return $field;

        // echo "<li>$field";var_dump($array);
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
            $entity = clone $template_entity;
            $entity['key'] = $row;
            $entity['timestamp'] = $row;
            $entity[Target_Pgsql::VIEW_MUTABLE] = $row;
            $entity[Target_Pgsql::VIEW_MUTABLE] = $row;

            $entities[] = $entity;
        }
        
        return $entities;
    }

    public function selector_security(Entity_Row $entity, Selector $selector)
    {
        $security = $selector->security;
        
        foreach(array(Target_Pgsql::VIEW_IMMUTABLE, Target_Pgsql::VIEW_MUTABLE) as $view_name)
        {
            foreach($entity[$view_name] as $column_name => $column)
            {
                $security->allow($column_name);
            }
        }
    }
    
}
