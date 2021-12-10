import pandas as pd
import datenscreening as ds

conn, cur = ds.connection_and_cursor()

df = ds.initial_df(cur)
df = ds.drop_non_relevant(df)
df_dict = ds.make_df_dict(df)
df_dict = ds.convert_and_delete(df_dict)

def group_over_days (df: pd.DataFrame):
    group_by = df.groupby([pd.Grouper(freq="1D", key="created")]).sum("state")


if __name__ == "__main__":
    print(df_dict["sensor.dht22_temperature"].info())
    print(df_dict["sensor.dht22_temperature"])
    print("test", group_over_days(df_dict["sensor.dht22_temperature"]))