import pymysql
import pandas as pd

if __name__ == "__main__":
    #configuration
    host='telecommunications-instance-1.coirayad3xsn.us-east-2.rds.amazonaws.com'
    username='admin'
    password='password'
    port=3306
    database_name='telecom'

    #connect
    connection=pymysql.connect(host=host, user=username, port=port, passwd=password, db=database_name)
    cursor=connection.cursor()
    cursor.execute("select count(*) from employees")

    #read the text files to pass to sql tables
    employees_table=pd.read_csv(r'data\employees.txt', sep='\t')
    users_table=pd.read_csv(r"data\users.txt", sep='\t')
    phone_calls_table=pd.read_csv(r"data\phone_calls.txt", sep='\t')
    finances_table=pd.read_csv(r"data\finance.txt", sep='\t')

    #pass to tables
    #employees
    for i in range(len(employees_table)):
        sql = "INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)"
        temp=list(employees_table.iloc[i,:-1])
        temp.append(int(employees_table.iloc[i,-1]))
        cursor.execute(sql, temp)
    connection.commit()

    #users
    for i in range(len(users_table)):
        sql = "INSERT INTO users VALUES (%s, %s, %s, %s, %s, %s, %s)"
        temp=list(users_table.iloc[i,:-1])
        temp.append(int(users_table.iloc[i,-1]))
        cursor.execute(sql, temp)
    connection.commit()

    #phone_calls
    for i in range(len(phone_calls_table)):
        sql = "INSERT INTO phone_calls VALUES (%s, %s, %s, %s, %s, %s, %s)"
        temp=list(phone_calls_table.iloc[i,:])
        t=[temp[0], temp[1], temp[2], float(temp[3]), float(temp[4]), float(temp[5]), temp[6]]
        cursor.execute(sql, t)
    connection.commit()

    #finances
    for i in range(len(finances_table)):
        sql = "INSERT INTO finances VALUES (%s, %s, %s, %s, %s, %s, %s)"
        temp=list(finances_table.iloc[i,:])
        temp=[temp[0], int(temp[1]), int(temp[2]), temp[3], int(temp[4]), int(temp[5]), int(temp[6])]
        cursor.execute(sql, t)
    connection.commit()


