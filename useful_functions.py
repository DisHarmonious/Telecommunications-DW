import pandas as pd
from faker import Faker
import random
from shapely import geometry
from shapely.geometry import Polygon
from random_timestamp import random_timestamp
import numpy as np

fake = Faker()

def create_employees(num, company):
    # columns=['employee_id', 'name', 'address', 'email', 'phone_number', 'company']
    df = []
    for i in range(num):
        temp = [
                fake.name(),
                fake.address(),
                fake.email(),
                fake.phone_number(),
                company
                ]
        df.append(temp)
    return df

def create_users(num, employee):
    # columns=['user_id', 'name', 'address', 'phone_number', 'sub_data', 'employee', 'billing_packet']
    df = []
    for i in range(num):
        temp = [
                fake.name(),
                fake.address(),
                fake.phone_number(),
                pd.date_range('2017-1-1', periods=151)[random.randint(0, 150)].date(),
                employee
                ]
        df.append(temp)
    return df

def create_phone_calls(num, user, a, year, month):
    df = []
    for i in range(num):
        temp = [
                user,
                a[random.randint(0, len(a)-1)],
                random.randint(1, 300),
                round(random.uniform(5.0, 11.0), 1),
                round(random.uniform(45.0, 48.0), 1),
                random_timestamp(year=year, month=month)
                ]
        df.append(temp)
    return df


def calculate_monthly_billing(user, packet, phone_calls, employee, company, year, month):
    if packet == 0:
        #determine domestic and internatioanl calls
        poly = geometry.Polygon([(6, 45), (10, 45), (6, 47), (10, 47)])
        domestic_calls=[]
        international_calls=[]
        for i in range(len(phone_calls)):
            point=geometry.Point(phone_calls['geo_x_caller'].iloc[i], phone_calls['geo_y_caller'].iloc[i])
            if poly.contains(point):
                domestic_calls.append(phone_calls['real_duration'].iloc[i])
            else:
                international_calls.append(phone_calls['real_duration'].iloc[i])
        #determine duration of domestic calls
        duration=0
        if len(domestic_calls)>0:
            for i in range(len(domestic_calls)):
                if domestic_calls[i] < 60:
                    domestic_calls[i]=60
                duration = duration + domestic_calls[i]
        #determine number of international calls
        international_call_billing=len(international_calls)*10
        #formula for packet 0:
        if duration > 300:
            duration=duration-300
            billing = international_call_billing + 10 + np.ceil(duration/60)*2
        else:
            billing = international_call_billing + 10
        ######################################################
    elif packet == 1:
        billing = len(phone_calls)*5
        ######################################################
    elif packet == 2:
        billing = 50
        ######################################################
    elif packet == 3:
        #determine domestic and internatioanl calls
        poly = geometry.Polygon([(6, 45), (10, 45), (6, 47), (10, 47)])
        domestic_calls=[]
        international_calls=[]
        for i in range(len(phone_calls)):
            point=geometry.Point(phone_calls['geo_x_caller'].iloc[i], phone_calls['geo_y_caller'].iloc[i])
            if poly.contains(point):
                domestic_calls.append(phone_calls['real_duration'].iloc[i])
            else:
                international_calls.append(phone_calls['real_duration'].iloc[i])
        #determine duration of domestic calls
        duration=0
        if len(domestic_calls)>0:
            i=0
            for i in range(len(domestic_calls)):
                if domestic_calls[i] < 60:
                    domestic_calls[i]=60
                duration = duration + domestic_calls[i]
        #determine number of international calls
        international_call_billing=len(international_calls)*5
        #formula for packet 3:
        if duration > 1200:
            duration=duration-1200
            billing = international_call_billing + 20 + np.ceil(duration/60)*2
        else:
            billing = international_call_billing + 20
        ######################################################
    else:
        #determine domestic and internatioanl calls
        poly = geometry.Polygon([(6, 45), (10, 45), (6, 47), (10, 47)])
        domestic_calls=[]
        international_calls=[]
        for i in range(len(phone_calls)):
            point=geometry.Point(phone_calls['geo_x_caller'].iloc[i], phone_calls['geo_y_caller'].iloc[i])
            if poly.contains(point):
                domestic_calls.append(phone_calls['real_duration'].iloc[i])
            else:
                international_calls.append(phone_calls['real_duration'].iloc[i])
        #determine duration of domestic calls
        duration_domestic=0
        if len(domestic_calls)>0:
            for i in range(len(domestic_calls)):
                duration_domestic = duration_domestic + domestic_calls[i]
        #determine number of international calls
        duration_international=0
        if len(international_calls)>0:
            for i in range(len(international_calls)):
                duration_international = duration_international + international_calls[i]
        international_call_billing=duration_international*0.1
        #formula for packet 4:
        if duration_domestic > 600:
            duration=duration_domestic-600
            billing = international_call_billing + 15 + duration*0.01
            billing=np.ceil(billing)
        else:
            billing = international_call_billing + 15
            billing=np.ceil(billing)
    ######################################################
    df=[user, billing, packet, employee, company, year, month]
    return (df)
