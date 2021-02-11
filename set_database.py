import mysql.connector
import pandas as pd

if __name__ == "__main__":
    #connect to sql
    sql_check = 1
    if sql_check == 1:
        mydb = mysql.connector.connect(
            host="localhost",
            database="telecom",
            user="root",
            password="password"
        )
        mycursor = mydb.cursor()
        print("using telecom database")
        cursor=mydb.cursor()
        cursor.execute("SELECT COUNT(*) FROM employees")
        id_counter=cursor.fetchone()[0]
        print(id_counter)
        sql_check=0

    #read the text files to pass to sql tables
    employees_table=pd.read_csv(r'data\employees.txt', sep='\t')
    users_table=pd.read_csv(r"data\users.txt", sep='\t')
    phone_calls_table=pd.read_csv(r"data\phone_calls.txt", sep='\t')

    #pass to tables
    #employees
    for i in range(len(employees_table)):
        sql = "INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)"
        temp=list(employees_table.iloc[i,:-1])
        temp.append(int(employees_table.iloc[i,-1]))
        mycursor.execute(sql, temp)
    mydb.commit()
    print('ok')

    #users
    for i in range(len(users_table)):
        sql = "INSERT INTO users VALUES (%s, %s, %s, %s, %s, %s, %s)"
        temp=list(users_table.iloc[i,:-1])
        temp.append(int(users_table.iloc[i,-1]))
        mycursor.execute(sql, temp)
    mydb.commit()
    print('ok')

    #phone_calls
    for i in range(len(phone_calls_table)):
        sql = "INSERT INTO phone_calls VALUES (%s, %s, %s, %s, %s, %s, %s)"
        temp=list(phone_calls_table.iloc[i,:])
        t=[temp[0], temp[1], temp[2], float(temp[3]), float(temp[4]), float(temp[5]), temp[6]]
        mycursor.execute(sql, t)
        if i % 500000 == 0: print(i)
    mydb.commit()





