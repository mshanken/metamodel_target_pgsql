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

    private $select_data;
    private $select_index;
    private $select_entity;

    public function validate_entity(Entity $entity)
    {
        return $entity instanceof Target_Pgsqlable;    
    }

    /**
     * implements selectable
     */
    public function select(Entity $e, Selector $selector = null)
    {
        $this->select_deferred($e, $selector);
        $output = array();
        while ($curr = $this->next_row()) $output[] = $curr;
        return $output;
    }
    
    /**
     * implements selectable
     */
    public function select_count(Entity $entity, Selector $selector = null)
    {
        $info = $entity->target_pgsql_info();

        $sql = sprintf('SELECT count(*) AS count FROM %s', $info->getTable());            

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

        error_log($sql);
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
    public function create(Entity $entity) 
    {   
        $info = $entity->target_pgsql_info();
        $entity['pgsql_mutable']->validate();
        if (strlen($entity['pgsql_mutable']->problems()))
        {
            throw new HTTP_Exception_400($entity['pgsql_mutable']->problems());
        }

        $returning_fields = array_merge(
            array_keys($entity['pgsql_mutable']->getData())
            , array_keys($entity[$info->getKeyView()]->getData())
            , array_keys($entity[$info->getTimestampView()]->getData())
            , array_keys($entity['pgsql_immutable']->getData())
        );

        $mutable_keys = array_keys($entity['pgsql_mutable']->getData());

        if (!is_null($info->getCreateFunction())) 
        { 
            $sql = sprintf('SELECT %s FROM %s(:%s)',
                implode(', ', array_keys($entity['key']->getData()))
                , $info->getCreateFunction()
                , implode(', :', $mutable_keys)
            );
        } 
        else
        {
            $sql = sprintf('INSERT INTO %s (%s) VALUES (:%s) RETURNING %s'
                , $info->getTable() 
                , implode(', ', $mutable_keys)
                , implode(', :', $mutable_keys) 
                , implode(', ', array_keys($entity['key']->getData()))
            );
            
        }
        $query = $this->query(Database::SELECT, $sql);
        $query->parameters($this->PDO_params($entity['pgsql_mutable']));

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
    public function update(Entity $entity, Selector $selector)
    {
        $info = $entity->target_pgsql_info();
        $entity['pgsql_mutable']->validate();
        if (strlen($entity['pgsql_mutable']->problems()))
        {
            throw new HTTP_Exception_400(var_export($entity['pgsql_mutable']->problems()));
        }

        $returning_fields = array_merge(
            array_keys($entity['pgsql_mutable']->getData())
            , array_keys($entity[$info->getKeyView()]->getData())
            , array_keys($entity[$info->getTimestampView()]->getData())
            , array_keys($entity['pgsql_immutable']->getData())
        );

        if (!is_null($info->getUpdateFunction())) 
        { 
            $sp_parameter_fields = array_merge(
                array_keys($entity[$info->getKeyView()]->getData())
                , array_keys($entity[$info->getTimestampView()]->getData())
                , array_keys($entity['pgsql_mutable']->getData())
            );
            $sql = sprintf('SELECT %s FROM %s(:%s)'
                , implode(', ', array_keys($entity['key']->getData()))
                , $info->getUpdateFunction()
                , implode(', :', $sp_parameter_fields)
            );
        } 
        else  
        {
            $sql = sprintf('UPDATE %s SET %s WHERE %s RETURNING %s'
                , $info->getTable()
                , implode(', ', array_map(
                    function($a) {return sprintf('"%s" = :%s', $a, $a);}
                    , array_keys($entity['pgsql_mutable']->getData())
                ))
                , $selector->build_target_query($entity, $this)
                , implode(', ', array_keys($entity['key']->getData()))
            );
        }
        $query = $this->query(Database::SELECT, $sql);
        $query->parameters($this->PDO_params($entity['pgsql_mutable']));
        $query->parameters($this->PDO_params($entity['pgsql_immutable']));
        $query->parameters($this->PDO_params($entity[$info->getKeyView()]));
        $query->parameters($this->PDO_params($entity[$info->getTimestampView()]));

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
    public function remove(Entity $e, Selector $selector)
    {
        $info = $e->target_pgsql_info();
        $where = $selector->build_target_query($e, $this);

        $sql = sprintf('DELETE FROM %s WHERE %s', $info->getTable(), $where);

        $query = $this->query(Database::DELETE, $sql);
        try 
        {
            return $query->execute();
        } catch (Exception $e) {
            $this->handle_exception($e);
        }
    }





    









    /**
     * take an array a return an array suitable for passing to PDO
     * array ('a' => 'b') outputs array(':a' => $this->encode($b));
     *
     * @TODO profile me, i suspect this is slow
     */
    protected function PDO_params(Entity_View $eview) 
    {
        if (count($eview->getData()))
        {
            return array_combine(
                array_map(function($a) { return ":$a";}
                    , array_keys($eview->getData())
                )
                , array_map(array($this, 'encode'), (array)$eview)
            );
        }
        return array();
    }




    // select helper
    public function select_deferred(Entity $entity, Selector $selector = null)
    {
        $info = $entity->target_pgsql_info();

        $returning_fields = array_merge(
            array_keys($entity[$info->getKeyView()]->getData())
            , array_keys($entity[$info->getTimestampView()]->getData())
            , array_keys($entity['pgsql_mutable']->getData())
            , array_keys($entity['pgsql_immutable']->getData())
        );
        
        if (is_null($info->getView())) {
            throw new HTTP_Exception_500('DEV ERROR, Target_Info has no view or table defined');
        }
        $sql = sprintf('SELECT %s FROM %s', implode(', ', $returning_fields), $info->getView());            

        if (!is_null($selector)) 
        {
            if ($where = $selector->build_target_query($entity, $this))
            {
                if ('()' != $where)
                {
                    $sql = sprintf('%s WHERE %s', $sql, $where);
                }
            }
            $sql = sprintf('%s %s %s', $sql, $selector->build_target_sort($entity, $this), $selector->build_target_page($entity, $this));
        }

        error_log($sql);
        $this->select_data = $this->_db->query(Database::SELECT, $sql)->as_array();
        $this->select_index = 0;
        $this->select_entity = get_class($entity);
    }

    public function next_row() 
    {
        if ($this->select_index < count($this->select_data))
        {
            $row = $this->select_data[$this->select_index++];
            $row = $this->decode($row);
            $entity = new $this->select_entity();
            $info = $entity->target_pgsql_info();
            $entity[$info->getKeyView()]->setData($row);
            $entity[$info->getTimestampView()]->setData($row);
            $entity['pgsql_mutable']->setData($row);
            $entity['pgsql_immutable']->setData($row);

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
    public function visit_exact($entity, $column_storage_name, $param)
    {
        $column_name = $this->visit_column_name($entity, $column_storage_name);
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
    public function visit_search($entity, $column_storage_name, $param) 
    {
        $column_name = $this->visit_column_name($entity, $column_storage_name);
        return sprintf("(%s ILIKE '%%%s%%')", $column_name, pg_escape_string($param));
        break;
    }

    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_max($entity, $column_storage_name, $param) 
    {
        $column_name = $this->visit_column_name($entity, $column_storage_name);
        return sprintf("(%s <= %d)", $column_name, $param);
        break;
    }

    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_min($entity, $column_storage_name, $param) 
    {
        $column_name = $this->visit_column_name($entity, $column_storage_name);
        return sprintf("(%s >= %d)", $column_name, $param);
        break;
    }

    /**
     * satisfy selector visitor interface
     *
     */
    public function visit_range($entity, $column_storage_name, $min, $max) 
    {
        $column_name = $this->visit_column_name($entity, $column_storage_name);
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
     * Helper for the visit_*() interface that builds WHERE clauses out of selectors.
     * Responsible for looking up an actual column name as it is seen by Postgres.
     */
    private function visit_column_name($entity, $column_storage_name)
    {
        foreach(array('key', 'timestamp', 'pgsql_immutable', 'pgsql_mutable') as $view_name)
        {
            $column_name = $entity[$view_name]->lookup_storage_name($column_storage_name);
            if($column_name) return $column_name;
        }
        
        throw new HTTP_Exception_400("Unknown column \"" . $column_storage_name . "\" in entity \"" . $entity->getName() . "\".");
    }

    /**
     * choke point for overriding/caching and logging of queries
     *
     */
    private function query($mode, $sql)
    {
        error_log( $sql );
        return DB::query($mode, $sql);    
    }





    /**
     * all necessary transforms to put a column into pgsql
     * complex data are flattened into pgsql arrays
     * @TODO migrate away from pgsql array to json format
     */
    public function encode(Entity_Columnable $column)
    {
        // redundant in a recursive function ?
        if (!$column->validate(false)) throw new Exception(sprintf('your data, %s, for column, %s, is invalid', $column->getData(), get_class($column->getType())));

        $value = $column->getData();
        $type = $column->getType();

        // override point allows type to hijack their encoding...
        if (method_exists($type, 'transform_target_pgsql'))
        {
            return $type->transform_target_pgsql($value);
        } 
        // flatten into an psql array
        else if ($column instanceof Traversable)
        {
            $tmp = array();
            foreach ($column as $k => $v) 
            {
               // $tmp[] = trim($this->encode($v),'"');
               $tmp[] = addslashes($this->encode($v));
            }
            if(count($tmp) == 0) return "{}";
            return sprintf('{"%s"}', implode('","', $tmp));
        }
        // this does not recurse because we only encode Entity_Columnable/Traversable Objects
        else if (is_array($value))
        {
            return sprintf('{%s}', implode(',', $value));
        }
        else if ($type instanceof Type_Number) 
        {
            return $value;
        }
        else if ($type instanceof Type_String) 
        {
            $value = trim($value);
            if (empty($value))
            {
                return NULL; // a null char
                return 'NULL'; // a literal
            }

            return $value;
            return pg_escape_string($value);
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
    public function select_custom(Entity $template_entity, $sql, array $params = array()) 
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
        $info = $template_entity->target_pgsql_info();
        foreach($results as $row) 
        {
            $entity = clone $template_entity;
            $entity[$info->getKeyView()]->setData($row);
            $entity[$info->getTimestampView()]->setData($row);
            $entity['pgsql_mutable']->setData($row);
            $entity['pgsql_immutable']->setData($row);

            $entities[] = $entity;
        }
        
        return $entities;
    }

    public function selector_security(Entity $entity, Selector $selector)
    {
        $security = $selector->security;
        
        foreach($entity['selector'] as $column_name => $column)
        {
            $security->allow($column_name);
        }
    }
    
}
