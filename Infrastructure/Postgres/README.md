### PostgreSql Library

This project is an implementation of Psycopg2 with some functions to make easier the connection and database operations with PostgreSql Database.

To use this library first you need install Psycopg2, base library of this project. It is strongly recommended that you add this library to your project requirements. To install by terminal use this command:

```
    pip install psycopg2
```

To connect to PostgreSql Database some params must be informed as Host Address, port, username and passsword. This params must be informed by environment variables. The following variables must be informed: 

 * POSTGRE_HOST
 * POSTGRE_PORT
 * POSTGRE_USERNAME
 * POSTGRE_PASSWORD


To use the library on python, follow the example: 

```python
    
    # Importing module
    from Postgre import Postgre    
    
    # Create an instance of Postgre object setting database name
    # and connection timeout
    db = Postgre(database='DbName', timeout=180)
    
    # Functions available:
    db.insert(query: str, params: list)
    db.bulk_insert(table: str, data: str,  columns: tuple, sep: str='\t')
    db.execute_query(query: str)
    db.read(query: str, params: tuple = None, to_dict: bool = False)

``` 



