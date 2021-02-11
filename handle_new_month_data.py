'''
####################### HANDLING NEW DATA #######################
-new phone calls are created
-calculate billing for users
-pass data updating local database
-pass data updating aurora database
'''

import pandas as pd
import pyodbc, pymysql, random, uuid
from useful_functions import create_phone_calls, calculate_monthly_billing


if __name__ == "__main__":
    #import data
    cnxn = pyodbc.connect("DRIVER={MySQL ODBC 8.0 ANSI Driver};SERVER=localhost;DATABASE=telecom;USER=root;PASSWORD=password")
    cursor=cnxn.cursor()
    packets= pd.read_sql("SELECT user_id, billing_packet FROM users;",cnxn)
    users=packets['user_id']
    employees= pd.read_sql("SELECT user_id, employee FROM users;",cnxn)
    companies=pd.read_sql("SELECT employee_id, company FROM employees;",cnxn)

    #new  month's phone calls are created
    phone_calls_table = []
    for year in [2021]:
        for month in [1]:
            for i in range(len(users)):
                temp = create_phone_calls(random.randint(0, 20), users.iloc[i], users, year, month)
                phone_calls_table = phone_calls_table + temp
    newmonth_df = pd.DataFrame.from_records(phone_calls_table)
    id = newmonth_df.apply(lambda x: uuid.uuid1(), axis=1)
    newmonth_df.insert(loc=0, column='id', value=id)
    newmonth_df.columns=["call_id", "caller_id", "receiver_id", "real_duration", "geo_x_caller", "geo_y_caller", "timestamp"]
    nmdf=newmonth_df
    newmonth_df['year']=[2021]*len(newmonth_df)
    newmonth_df['month']=[1]*len(newmonth_df)
    newmonth_df.drop(columns=['timestamp'])

    #new month's billing is calculated
    finance=[]

    for year in [2021]:
        for month in [1]:
            for i in range(len(users)):
                user=users.iloc[i]
                small_df=newmonth_df[newmonth_df['caller_id']==user]
                packet=packets[packets['user_id']==user].iloc[0][1]
                employee=employees[employees['user_id']==user].iloc[0][1]
                company=companies[companies['employee_id']==employee]
                company=company['company']
                temp=calculate_monthly_billing(user, packet, small_df, employee, company, year, month)
                finance.append(temp)

    #pass new phone calls, and new month's bills to local db
    for i in range(len(nmdf)):
        sql = "INSERT INTO phone_calls VALUES (%s, %s, %s, %s, %s, %s, %s)"
        temp=list(nmdf.iloc[i,:])
        t=[temp[0], temp[1], temp[2], float(temp[3]), float(temp[4]), float(temp[5]), temp[6]]
        cursor.execute(sql, t)
    cnxn.commit()
    for i in range(len(finance)):
        sql = "INSERT INTO finances VALUES (%s, %s, %s, %s, %s, %s, %s)"
        temp=finance[i]
        temp=[temp[0], int(temp[1]), int(temp[2]), temp[3], int(temp[4]), int(temp[5]), int(temp[6])]
        cursor.execute(sql, temp)
    cnxn.commit()

    #pass new phone calls, and new month's bills to aurora db
    host='telecommunications-instance-1.coirayad3xsn.us-east-2.rds.amazonaws.com'
    username='admin'
    password='password'
    port=3306
    database_name='telecom'
    connection=pymysql.connect(host=host, user=username, port=port, passwd=password, db=database_name)
    cursor=connection.cursor()
    for i in range(len(phone_calls_table)):
        sql = "INSERT INTO phone_calls VALUES (%s, %s, %s, %s, %s, %s, %s)"
        temp=list(nmdf.iloc[i,:])
        t=[temp[0], temp[1], temp[2], float(temp[3]), float(temp[4]), float(temp[5]), temp[6]]
        cursor.execute(sql, t)
    connection.commit()
    for i in range(len(finance)):
        sql = "INSERT INTO finances VALUES (%s, %s, %s, %s, %s, %s, %s)"
        temp=finance[i]
        temp=[temp[0], int(temp[1]), int(temp[2]), temp[3], int(temp[4]), int(temp[5]), int(temp[6])]
        cursor.execute(sql, t)
    connection.commit()








