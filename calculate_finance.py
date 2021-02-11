import pandas as pd
from useful_functions import calculate_monthly_billing
import mysql.connector
import pyodbc
import datetime

if __name__ == "__main__":
    #connect to sql
    cnxn = pyodbc.connect("DRIVER={MySQL ODBC 8.0 ANSI Driver};SERVER=localhost;DATABASE=telecom;USER=root;PASSWORD=password")
    sql="SELECT caller_id, real_duration, geo_x_caller, geo_y_caller, timestamp FROM phone_calls"
    data = pd.read_sql(sql,cnxn)
    cursor=cnxn.cursor()

    #find users, their billing packets, their corresponding employee, and the memployees company
    packets= pd.read_sql("SELECT user_id, billing_packet FROM users;",cnxn)
    employees= pd.read_sql("SELECT user_id, employee FROM users;",cnxn)
    companies=pd.read_sql("SELECT employee_id, company FROM employees;",cnxn)

    #get month and year
    month=[0]*len(data)
    year=[0]*len(data)
    for i in range(len(data)):
        month[i]=datetime.datetime.strptime(str(data['timestamp'].iloc[i]), "%Y-%m-%d").month
        year[i]=datetime.datetime.strptime(str(data['timestamp'].iloc[i]), "%Y-%m-%d").year
    data['month']=month
    data['year']=year
    data.drop(columns=['timestamp'])

    #do calculations
    finance=[]

    for year in [2017]:
        for month in [6,7,8,9,10,11,12]:
            for i in range(len(packets)):
                user=packets['user_id'].iloc[i]
                small_df=data[data['month']==month]
                small_df=small_df[small_df['year']==year]
                small_df=small_df[small_df['caller_id']==user]
                packet=packets[packets['user_id']==user].iloc[0][1]
                employee=employees[employees['user_id']==user].iloc[0][1]
                company=companies[companies['employee_id']==employee]
                company=company['company']
                temp=calculate_monthly_billing(user, packet, small_df, employee, company, year, month)
                finance.append(temp)
            print(month)
        print(year)

    for year in [2018, 2019, 2020]:
        for month in [1,2,3,4,5,6,7,8,9,10,11,12]:
            for i in range(len(packets)):
                user=packets['user_id'].iloc[i]
                small_df=data[data['month']==month]
                small_df=small_df[small_df['year']==year]
                small_df=small_df[small_df['caller_id']==user]
                packet=packets[packets['user_id']==user].iloc[0][1]
                employee=employees[employees['user_id']==user].iloc[0][1]
                company=companies[companies['employee_id']==employee]
                company=company['company']
                temp=calculate_monthly_billing(user, packet, small_df, employee, company, year, month)
                finance.append(temp)

            print(month)
        print(year)

    #save locally
    df = pd.DataFrame.from_records(finance)
    df.to_csv(r"data\finance.txt", header=None, index=None, sep='\t', mode='a')

    #pass to db
    mydb = mysql.connector.connect(host="localhost",database="telecom",user="root",password="password")
    mycursor = mydb.cursor()

    for i in range(len(df)):
        sql = "INSERT INTO finances VALUES (%s, %s, %s, %s, %s, %s, %s)"
        temp=list(df.iloc[i,:])
        temp=[temp[0], int(temp[1]), int(temp[2]), temp[3], int(temp[4]), int(temp[5]), int(temp[6])]
        mycursor.execute(sql, temp)
        if i%50000==0: print(i)
    mydb.commit()







