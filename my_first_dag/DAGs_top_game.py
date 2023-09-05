import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

default_args = {
    'owner': 'a-zalivatskaja-31',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 16),
    'schedule_interval': '0 12 * * *'
}
   
@dag(default_args=default_args, catchup=False)
def a_zalivatskaja_vgsales_3():
    @task()
    def get_data():
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        my_year = 1994 + hash('a-zalivatskaja-31') % 23
        df.columns = df.columns.str.lower()
        df = df[df['year'].notna()]
        df.year = df.year.astype('int')
        df = df[df.year == my_year].reset_index()
        return df
    @task()
    def top_game_world(df):
        top_game = df.sort_values(by='global_sales', ascending=False).head(1).name.to_list()[0]
        return top_game
    @task()
    #Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    def top_eu_genre(df):
            top_list_eu = df.groupby('genre').eu_sales.sum().sort_values(ascending=False).head(5).index.to_list()
            return top_list_eu
    @task()
    #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    #Перечислить все, если их несколько
    def top_platform_am(df):
            top_platforms = df.loc[df.na_sales >= 1] \
                                    .groupby('platform') \
                                    .name.nunique() \
                                    .sort_values(ascending=False)
            top_platforms = top_platforms[top_platforms == top_platforms.max()].index.to_list()
            return top_platforms

    @task()
    #У какого издателя самые высокие средние продажи в Японии?
    #Перечислить все, если их несколько
    def get_top_jp(df):
        top_jp = df.groupby('publisher', as_index=False).agg({'jp_sales':'mean'})
        max_jp_mean_sales = top_jp.jp_sales.max()
        top_jp = top_jp[top_jp.jp_sales == max_jp_mean_sales]
        top_jp = top_jp.to_string(index=False)
        return top_jp

    @task()
    #Сколько игр продались лучше в Европе, чем в Японии?
    def eu_jp_games(df):
        eu_jp = df.loc[df.eu_sales > df.jp_sales].name.nunique()
        return eu_jp

    @task()
    def print_data(top_game, top_list_eu, top_platforms, top_jp, eu_jp):
        context = get_current_context()
        date = context['ds']
        my_year = 1994 + hash('a-zalivatskaja-31') % 23

        print(f'''
            Date: {date}
            -1- Top sales game worldwide in {my_year}: {top_game}
            -2- Top genre in EU in {my_year}: {top_list_eu}
            -3- Top platform in North America in {my_year}: {top_platforms}
            -4- Top publisher in Japan in {my_year}: {top_jp}
            -4- Number of Games EU vs. JP in {my_year}: {eu_jp}''')     

    df = get_data()
    
    top_game = top_game_world(df)
    top_list_eu = top_eu_genre(df)
    top_platforms = top_platform_am(df)
    top_jp = get_top_jp(df)
    eu_jp = eu_jp_games(df)

    print_data(top_game, top_list_eu, top_platforms, top_jp, eu_jp)

a_zalivatskaja_vgsales_3 = a_zalivatskaja_vgsales_3()