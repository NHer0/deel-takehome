QUERY = """
    select
        {id_column} as id,
        {target_column} as value,
        ({target_column} - {threshold}) as difference  
    from {database}.{schema}.{table_name}
    where 1 = 1
        and {target_column} > {threshold}
        and {date_column} > '{start_date}'
    order by difference desc
"""