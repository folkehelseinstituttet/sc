# sc 7.0.8

* `task_inline_v1` allows for easy inline task creation
* Corresponding RStudio addin for inline tasks that copy from one db table to another

# sc 7.0.7

* `copy_into_new_table_where` allows for the creation of a new table from an old table
* Including `task_from_config_v2` 
* First RStudio addin

# sc 7.0.6

* write_data_infile now checks for Infinite/NaN values and sets them to NA

# sc 7.0.5

* `Task` now includes `action_before_fn` and `action_after_fn`

# sc 7.0.4

* `validator_field_contents_sykdomspulsen` now allows `baregion` as a valid `granularity_geo`

# sc 7.0.3

* `tm_get_plans_argsets_as_dt` provides an overview of the plans and argsets within a task

# sc 7.0.2

* `keep_rows_where` now also retains the PK constraints
