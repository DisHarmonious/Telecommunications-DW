from useful_functions import create_employees, create_users, create_phone_calls
import random
import uuid
import pandas as pd

if __name__ == "__main__":
    # create employees_table
    #providers = ['Lion', 'Hawk', 'Bear']
    df1 = create_employees(random.randint(20, 50), 0)
    df2 = create_employees(random.randint(20, 50), 1)
    df3 = create_employees(random.randint(20, 50), 2)
    employees_table = []
    employees_table = employees_table + df1
    employees_table = employees_table + df2
    employees_table = employees_table + df3
    print(len(employees_table))
    df = pd.DataFrame(employees_table)
    id = df.apply(lambda x: uuid.uuid1(), axis=1)
    df.insert(loc=0, column='id', value=id)
    print(df)
    df.to_csv(r'data\employees.txt', header=None, index=None, sep='\t', mode='a')

    # create users table
    users_table = []
    for i in range(len(employees_table)):
        temp = create_users(random.randint(20, 100), df['id'].iloc[i])
        for j in temp:
            if employees_table[i][4] == 0:
                j.append(random.randint(0, 2))
            elif employees_table[i][4] == 1:
                j.append(random.randint(1, 3))
            else:
                j.append(random.randint(2, 4))
        users_table = users_table + temp
    print(len(users_table))
    df = pd.DataFrame.from_records(users_table)
    id = df.apply(lambda x: uuid.uuid1(), axis=1)
    df.insert(loc=0, column='id', value=id)
    df.to_csv(r"data\users.txt", header=None, index=None, sep='\t', mode='a')
    print(df)

    # create phone calls
    phone_calls_table = []
    a=df.iloc[:,0]
    for year in [2017]:
        for month in [6, 7, 8, 9, 10, 11, 12]:
            for i in range(len(df)):
                temp = create_phone_calls(random.randint(0, 20), df.iloc[i,0], a, year, month)
                phone_calls_table = phone_calls_table + temp
        print(year)
    for year in [2018, 2019, 2020]:
        for month in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]:
            for i in range(len(df)):
                temp = create_phone_calls(random.randint(0, 20), df.iloc[i,0], a, year, month)
                phone_calls_table = phone_calls_table + temp
            print(month)
        print(year)
    df = pd.DataFrame.from_records(phone_calls_table)
    id = df.apply(lambda x: uuid.uuid1(), axis=1)
    df.insert(loc=0, column='id', value=id)
    df.to_csv(r"phone_calls.txt", header=None, index=None, sep='\t', mode='a')
