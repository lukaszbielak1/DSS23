import pyodbc 
import os
from names_generator import generate_name
import time

#connection string needs to be in environment variables
connection_string = os.environ["AZURE_SQL_CONNECTIONSTRING"]
conn = pyodbc.connect(connection_string)

starttime = time.monotonic()
while True:
    fullname = generate_name(style='capital')
    name = fullname.split(" ")[0]
    surname = fullname.split(" ")[1]
    qry = f"""INSERT INTO SalesLT.Customer (NameStyle, FirstName, LastName, EmailAddress, PasswordHash, PasswordSalt, rowguid, ModifiedDate) 
VALUES (0,  '{name}','{surname}','{name}.{surname}@dss23.com','***','***',NEWID(), GETDATE());
"""
    cursor = conn.cursor()
    cursor.execute(qry)
    conn.commit()
    cursor.close()
    print(qry)
    print(f"{fullname} inserted")
    time.sleep(0.5)
