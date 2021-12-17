import mariadb #DB Handling
import plotly #plotting #1
import pandas as pd #Dataframes
import matplotlib as plt #plotting #2

import sys
import numpy
numpy.set_printoptions(threshold=sys.maxsize)


def connection_and_cursor(user="bi2021", password="businessintelligence", host="212.201.138.114", port=3306, database="homeassistant"):
    conn = mariadb.connect(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database # only interesting database
    )

    cur = conn.cursor()

    return conn, cur

def initial_df(cur):
    cur.execute("show columns from states")

    cols = []

    for i in cur:
        cols.append(i[0])

    print("Spalten in Columns:", cols) # all columns in states

    cur.execute("SELECT * FROM states")
    df = pd.DataFrame(data=cur.fetchall(), columns=cols)
    print(df.info())
    
    return df

def drop_non_relevant(df):
    print("dropped", len(df[df["entity_id"]=="person.fschmidmichels"]), "from 'person.fschmidmichels'")
    df = df.drop(df[df["entity_id"]=="person.fschmidmichels"].index, 0)
    print("dropped", len(df[df["entity_id"]=="weather.langer_weg"]), "from 'weather.langer_weg'")
    df = df.drop(df[df["entity_id"]=="weather.langer_weg"].index, 0)
    print("dropped", len(df[df["entity_id"]=="zone.home"]), "from 'zone.home'")
    df = df.drop(df[df["entity_id"]=="zone.home"].index, 0)
    print("dropped", len(df[df["entity_id"]=="print_entities_to_file"]), "from 'print_entities_to_file'")
    df = df.drop(df[df["entity_id"]=="script.print_entities_to_file"].index, 0)
    print("dropped", len(df[df["entity_id"]=="wlan_switch_energy_totalstarttime"]), "from 'wlan_switch_energy_totalstarttime'")
    df = df.drop(df[df["entity_id"]=="sensor.wlan_switch_energy_totalstarttime"].index, 0)
    print("dropped", len(df[df["entity_id"]=="wlan_switch_status"]), "from 'wlan_switch_status'")
    df = df.drop(df[df["entity_id"]=="sensor.wlan_switch_status"].index, 0)
    print("dropped", len(df[df["entity_id"]=="switch.smartplug3_230v"]), "from 'switch.smartplug3_230v'")
    df = df.drop(df[df["entity_id"]=="switch.smartplug3_230v"].index, 0)
    print("dropped", len(df[df["entity_id"]=="sensor.smartplug3_energy_totalstarttime"]), "from 'sensor.smartplug3_energy_totalstarttime'")
    df = df.drop(df[df["entity_id"]=="sensor.smartplug3_energy_totalstarttime"].index, 0)
    print("dropped", len(df[df["entity_id"]=="sensor.smartplug3_status"]), "from 'sensor.smartplug3_status'")
    df = df.drop(df[df["entity_id"]=="sensor.smartplug3_status"].index, 0)
    print("dropped", len(df[df["entity_id"]=="switch.smartplug3_usb"]), "from 'switch.smartplug3_usb'")
    df = df.drop(df[df["entity_id"]=="switch.smartplug3_usb"].index, 0)
    print("dropped", len(df[df["entity_id"]=="sun.sun"]), "from 'sun.sun'")
    df = df.drop(df[df["entity_id"]=="sun.sun"].index, 0)
    print("dropped", len(df[df["entity_id"]=="binary_sensor.updater"]), "from 'binary_sensor.updater'")
    df = df.drop(df[df["entity_id"]=="binary_sensor.updater"].index, 0)
    
    ######from here on in excel unmentioned entityids######

    print("dropped", len(df[df["entity_id"]=="binary_sensor.wlan_switch_button1"]), "from 'binary_sensor.wlan_switch_button1'")
    df = df.drop(df[df["entity_id"]=="binary_sensor.wlan_switch_button1"].index, 0)
    print("dropped", len(df[df["entity_id"]=="persistent_notification.config_entry_discovery"]), "from 'persistent_notification.config_entry_discovery'")
    df = df.drop(df[df["entity_id"]=="persistent_notification.config_entry_discovery"].index, 0)
    print("dropped", len(df[df["entity_id"]=="sensor.mh_z19_temperature"]), "from 'sensor.mh_z19_temperature'")
    df = df.drop(df[df["entity_id"]=="sensor.mh_z19_temperature"].index, 0)
    print("dropped", len(df[df["entity_id"]=="switch.wlan_switch"]), "from 'switch.wlan_switch'")
    df = df.drop(df[df["entity_id"]=="switch.wlan_switch"].index, 0)

    return df

def make_df_dict(df):
    df_dict = {}
    for i in df["entity_id"].unique():
        print("convert to df_dict: entity_id:", i)
        #print("...", df[df["entity_id"]==str(i)])
        df_dict.update({i:df[df["entity_id"]==str(i)]})
    
    return df_dict

def convert_and_delete (df_dict):
    list_nonnum = []
    for i in df_dict:
        print("Aktuelle entity_id:", i)
        print(df_dict[i]["state"].head(), "\n")
        print("Anzahl unknown: ", df_dict[i][df_dict[i]["state"]=="unknown"].count())
        print("Anzahl unavailable: ", df_dict[i][df_dict[i]["state"]=="unavailable"].count())
        df_dict[i] = df_dict[i].drop(df_dict[i][df_dict[i]["state"]=="unknown"].index)
        df_dict[i] = df_dict[i].drop(df_dict[i][df_dict[i]["state"]=="unavailable"].index)

        try:             
            df_dict[i]["state"] = pd.to_numeric(df_dict[i]["state"])
            print(i, "Erfolgreich zu numerischen Werten konvertiert")

        except:
            print("Nicht numerisch: ", i)
            list_nonnum.append(str(i))
    #print(list_nonnum)
    for i in list_nonnum:
        try:
            df_dict[i].state.apply(str)
            list_nonnum.remove(i)
        except:
            print("non numeric nor string:", i)

        #print(df_dict[i]["state"].head(), "\n")
    print("Weder numerisch noch String:", list_nonnum)

    return df_dict

def close_conn(conn):
    conn.close()

if __name__ == "__main__":
    conn, cur = connection_and_cursor()
    df = initial_df(cur)
    df = drop_non_relevant(df)
    df_dict = make_df_dict(df)
    df_dict = convert_and_delete(df_dict)
    #for i in df_dict:
        #print(i, ":\n", df_dict[i].info())
    close_conn(conn)