<?php //defined('SYSPATH') or die('No direct access allowed.');
/**
 * target/pgsql/deferred.php
 * 
 * @package Metamodel
 * @subpackage Target
 * @author dchan@mshanken.com
 *
 **/
class Metamodel_Target_Pgsql_Memcache
extends Metamodel_Target_Pgsql
{
    
    /**
     * memcache 
     *
     * @var Memcache Object
     * @access protected
     */
    protected $memcache;

    /**
     * Constructor
     *
     * @param mixed $debugdb
     * @access public
     */
    public function __construct($debugdb = null)
    {
        parent::__construct($debugdb);

        $this->memcache = new Memcache;
        $this->memcache->connect(Kohana::$config->load('memcache.cache_host')
            , Kohana::$config->load('memcache.cache_port'));
    }

    /**
     * get_key
     *
     * @param Entity_Row $entity
     * @param Selector $selector
     * @access protected
     * @return string
     */
    protected function get_key(Entity_Row $entity, Selector $selector)
    {
        return sprintf('pg_%s_%s', $entity->get_root()->get_name() , $selector->render());
    }

    /**
     * get_cache
     *
     * @param Entity_Row $entity
     * @param Selector $selector
     * @access protected
     * @return array or false
     */
    protected function get_cache(Entity_Row $entity, Selector $selector)
    {
        $key = $this->get_key($entity, $selector);
        if ($cache = $this->memcache->get($key))
        {
            return unserialize($cache);
        }
        return false;
    }

    /**
     * set_cache
     *
     * @param Entity_Row $entity
     * @param Selector $selector
     * @param array $value
     * @access protected
     * @return bool
     */
    protected function set_cache(Entity_Row $entity, Selector $selector, array $value)
    {
        $key = $this->get_key($entity, $selector);
        if (count($value) == 1)
        {
            // return $this->memcache->set($key, serialize($results), MEMCACHE_COMPRESSED, 2592000);
            return $this->memcache->set($key, serialize($value), 0, Kohana::$config->load('memcache.db_cache_ttl'));
        }
        return false;
    }

    /**
     * select
     *
     * @param Entity_Row $entity
     * @param Selector $selector
     * @access public
     * @return array of Entity_Row
     */
    public function select(Entity_Row $entity, Selector $selector = null)
    {
        if (!($results = $this->get_cache($entity, $selector)))
        {
            $results = parent::select($entity, $selector);
            $this->set_cache($entity, $selector, $results);
        }
        return $results;
    }

    /**
     * update
     *
     * @param Entity_Row $entity
     * @param Selector $selector
     * @access public
     * @return Entity_Row
     */
    public function update(Entity_Row $entity, Selector $selector)    
    {
        $results = parent::update($entity, $selector);
        $this->set_cache($entity, $selector, $results);
        return $results;
    }

    /**
     * remove
     *
     * @param Entity_Row $entity
     * @param Selector $selector
     * @access public
     * @return bool
     */
    public function remove(Entity_Row $entity, Selector $selector)    
    {
        $this->memcache->delete($this->get_key($entity, $selector));
        return parent::remove($entity, $selector);
    }

}

