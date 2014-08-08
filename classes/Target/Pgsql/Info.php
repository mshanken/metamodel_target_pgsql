<?php

Trait Target_Pgsql_Info
{
    abstract public function pgsql_table();

    abstract public function default_selector();
 
    public function pgsql_view() 
    {
        return $this->pgsql_table();
    }

    public function pgsql_delete_function() 
    {
        return null;
    }

    public function pgsql_create_function()
    {
        return null;
    }

    public function pgsql_update_function()
    {
        return null;
    }

}
