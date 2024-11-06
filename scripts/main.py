import pandas as pd
from sqlalchemy import create_engine, text
from sensitive import *


def cleaning():
    df = pd.read_csv("data/employees.csv")

    df.columns = df.columns.str.lower().str.replace(" ", "_")
    df.rename(columns={"bonus_%": "bonus_percent"}, inplace=True)

    df["gender"] = df["gender"].fillna("Unknown")

    df["team"] = df["team"].fillna("Unknown")

    df["start_date"] = pd.to_datetime(df["start_date"]).dt.strftime("%d/%m/%y")

    df["last_login_time"] = pd.DatetimeIndex(df["last_login_time"]).astype("int64")

    df["senior_management"] = df["senior_management"].astype("bool")

    df.to_csv("data/employees_modified.csv", index=False)


def create_sub_tables(df, column):
    columns = ["id", column]
    vals = df[column].unique()
    ids = [i for i in range(1, len(vals) + 1)]
    data = [[a, b] for a, b in zip(ids, vals)]

    df = pd.DataFrame(data, columns=columns)

    return df


def dict_for_map(df, name):
    temp = df.to_dict()
    temp_id, temp_gender = temp["id"], temp[name]
    temp_dict = {key: val for val, key in zip(temp_id.values(), temp_gender.values())}
    return temp_dict


def dividing():
    employees_df = pd.read_csv("data/employees_modified.csv", index_col=False)

    gender_df = create_sub_tables(employees_df, "gender")

    team_df = create_sub_tables(employees_df, "team")

    gender_df.to_csv("data/gender_modified.csv", index=False)

    team_df.to_csv("data/team_modified.csv", index=False)

    gender_dict = dict_for_map(gender_df, "gender")
    employees_df["gender"] = employees_df["gender"].map(gender_dict)

    team_dict = dict_for_map(team_df, "team")
    employees_df["team"] = employees_df["team"].map(team_dict)

    employees_df.to_csv("data/employees_modified.csv", index=False)


def uploading(df, table):
    engine = create_engine(postgres_url)
    df.to_sql(table, engine, if_exists="replace", index=False)


def group_uploading():
    employees_df = pd.read_csv("data/employees_modified.csv", index_col=False)
    gender_df = pd.read_csv("data/gender_modified.csv", index_col=False)
    team_df = pd.read_csv("data/team_modified.csv", index_col=False)

    uploading(employees_df, "employees")
    uploading(gender_df, "gender")
    uploading(team_df, "team")


def linking_tables():
    engine = create_engine(postgres_url)
    with engine.connect() as conn:
        with conn.begin() as trans:
            conn.execute(text("alter table gender add primary key (id)"))
            conn.execute(text("alter table team add primary key (id)"))
            conn.execute(
                text(
                    "alter table employees add foreign key (gender) references gender(id);"
                )
            )
            conn.execute(
                text(
                    "alter table employees add foreign key (team) references team(id);"
                )
            )
            trans.commit()


def merged_display():
    engine = create_engine(postgres_url)
    employees_df = pd.read_sql_table("employees", engine)
    gender_df = pd.read_sql_table("gender", engine)
    team_df = pd.read_sql_table("team", engine)

    df = pd.merge(
        left=employees_df,
        right=gender_df,
        left_on="gender",
        right_on="id",
        how="left",
    )

    df = df.drop(["gender_x", "id"], axis=1)

    df = pd.merge(
        left=df,
        right=team_df,
        left_on="team",
        right_on="id",
        how="left",
    )

    df = df.drop(["team_x", "id"], axis=1)

    # df.to_csv("data/employees_finalized.csv", index=0)
    print(df.head())


def main():
    cleaning()
    dividing()
    group_uploading()
    # # linking_tables()
    merged_display()


if __name__ == "__main__":
    main()
