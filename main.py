from requests import get
import pandas as pd

from configparser import ConfigParser
from datetime import datetime
import time
import os
import sys

os.chdir("C:\\Users\\Mateo\\Documents\\Python_projects\\weather_report")

sys.path.append("C:\\Users\\Mateo\\Documents\\Python_projects\\weather_report")
sys.path.append("C:\\Users\\Mateo\\anaconda3")
sys.path.append("C:\\Users\\Mateo\\anaconda3\\python38.zip")
sys.path.append("C:\\Users\\Mateo\\anaconda3\\DLLs")
sys.path.append("C:\\Users\\Mateo\\anaconda3\\lib")
sys.path.append("C:\\Users\\Mateo\\anaconda3\\lib\\site-packages")
sys.path.append("C:\\Users\\Mateo\\anaconda3\\Scripts")
sys.path.append("C:\\Users\\Mateo\\anaconda3\\Library\\bin")
sys.path.append("C:\\Users\\Mateo\\anaconda3\\lib\\site-packages\\locket-0.2.1-py3.8.egg")
sys.path.append("C:\\Users\\Mateo\\anaconda3\\lib\\site-packages\\win32")
sys.path.append("C:\\Users\\Mateo\\anaconda3\\lib\\site-packages\\win32\\lib")
sys.path.append("C:\\Users\\Mateo\\anaconda3\\lib\\site-packages\\Pythonwin")
sys.path.append("C:\\Users\\Mateo\\anaconda3\\lib\\site-packages\\IPython\\extensions")
sys.path.append("C:\\Users\\Mateo\\.ipython")

# get credentials
parser = ConfigParser()
_ = parser.read('notebook.cfg')
api_auth_key = parser.get('my_api', 'auth_key')

FileNamePrefix = 'C:\\Users\\Mateo\\Documents\\Python_projects\\weather_report'
filename = FileNamePrefix + "\\Weather_Data.csv"

hist_data = pd.read_csv('{}\\weather_hist_data.csv'.format(FileNamePrefix), encoding='utf8')


# hist_data = pd.read_parquet('{}\\weather_hist_data.parquet'.format(FileNamePrefix))

# functions
def json_to_df(stream_data, search_data, column):
    """_summary_

    Args:
        stream_data (_type_): _description_
        search_data (_type_): _description_
        column (_type_): _description_

    Returns:
        _type_: _description_
    """
    stream_data = {search_data: stream_data[column][search_data] for search_data in search_data}
    stream_d2 = {k: str(v) for k, v in stream_data.items()}
    df_stream = pd.DataFrame(stream_d2, index=[0])
    return df_stream


def df_creatio(stream_data, column):
    """_summary_

    Args:
        stream_data (_type_): _description_
        column (_type_): _description_

    Returns:
        _type_: _description_
    """
    data = {column: stream_data[column]}
    df_stream = pd.DataFrame(data, index=[0])
    return df_stream


# current time
now = datetime.now()
now_dt = now.strftime('%Y-%m-%d %H:%M:%S')

# to get coordinates for different cities
# for i in list(cities_coordinates.keys()):
#    geo_url = "http://api.openweathermap.org/geo/1.0/direct?q={city_name}&limit=5&appid={API_key}".format(city_name = i,API_key=api_auth_key)
#    print(geo_url)
#    city_geo_data = get(geo_url)
#    city_geo_data = city_geo_data.json()
#    print(city_geo_data)


cities_coordinates = {'Krakow': {'lat': '50.0469432', 'lon': '19.997153435836697'},
                      'London': {'lat': '51.5073219', 'lon': '-0.1276474'},
                      'Amsterdam': {'lat': '52.3727598', 'lon': '4.8936041'},
                      'Dubai': {'lat': '25.2653471', 'lon': '55.2924914'},
                      'Tokio': {'lat': '35.6828387', 'lon': '139.7594549'},
                      'Shanghai': {'lat': '31.2322758', 'lon': '121.4692071'},
                      'Delhi': {'lat': '28.6517178', 'lon': '77.2219388'},
                      'Cairo': {'lat': '30.0443879', 'lon': '31.2357257'},
                      'Karachi': {'lat': '24.8546842', 'lon': '67.0207055'},
                      'Istanbul': {'lat': '41.0091982', 'lon': '28.9662187'},
                      'Lagos': {'lat': '6.4550575', 'lon': '3.3941795'},
                      'Sao Paulo': {'lat': '-23.5506507', 'lon': '-46.6333824'},
                      'Mexico City': {'lat': '19.4326296', 'lon': '-99.1331785'},
                      'Mumbai': {'lat': '19.0785451', 'lon': '72.878176'},
                      'Beijing': {'lat': '39.906217', 'lon': '116.3912757'},
                      'Dhaka': {'lat': '23.7644025', 'lon': '90.389015'},
                      'New York': {'lat': '40.7127281', 'lon': '-74.0060152'},
                      'Buenos Aires': {'lat': '-34.6075682', 'lon': '-58.4370894'},
                      'Manila': {'lat': '14.5948914', 'lon': '120.9782618'},
                      'Kinshasa': {'lat': '-4.3217055', 'lon': '15.3125974'},
                      'Tianjin': {'lat': '39.0856735', 'lon': '117.1951073'},
                      'Los Angeles': {'lat': '34.0536909', 'lon': '-118.242766'},
                      'Moscow': {'lat': '55.7504461', 'lon': '37.6174943'},
                      'Paris': {'lat': '48.8588897', 'lon': '2.3200410217200766'},
                      'Bogota': {'lat': '4.6534649', 'lon': '-74.0836453'},
                      'Jakarta': {'lat': '-6.1753942', 'lon': '106.827183'},
                      'Lima': {'lat': '-12.0621065', 'lon': '-77.0365256'},
                      'Bangkok': {'lat': '13.7524938', 'lon': '100.4935089'},
                      'Seoul': {'lat': '37.5666791', 'lon': '126.9782914'},
                      'Tehran': {'lat': '35.6892523', 'lon': '51.3896004'},
                      'Kuala Lumpur': {'lat': '3.1516964', 'lon': '101.6942371'},
                      'Riyadh': {'lat': '24.638916', 'lon': '46.7160104'},
                      'Baghdad': {'lat': '33.3061701', 'lon': '44.3872213'},
                      'Santiago': {'lat': '-33.4377756', 'lon': '-70.6504502'},
                      'Madrid': {'lat': '40.4167047', 'lon': '-3.7035825'},
                      'Toronto': {'lat': '43.6534817', 'lon': '-79.3839347'},
                      'Dar es Salaam': {'lat': '-6.8160837', 'lon': '39.2803583'},
                      'Miami': {'lat': '25.7741728', 'lon': '-80.19362'},
                      'Singapore': {'lat': '1.2899175', 'lon': '103.8519072'},
                      'Johannesburg': {'lat': '-26.205', 'lon': '28.049722'}}

polution_quality = {'1': 'Good', '2': 'Fair', '3': 'Moderate', '4': 'Poor', '5': 'Very Poor'}

search_weatcher_data = ['id', 'main', 'description', 'icon']
search_wind_data = ['speed', 'deg']
search_main_data = ['temp', 'feels_like', 'temp_min', 'temp_max', 'pressure', 'humidity']
search_sys_data = ['country', 'sunrise', 'sunset']  # 'type', 'id',
search_coord_data = ['lon', 'lat']
search_cloud = ['all']
# pollution_aqi = ['aqi']
pollution_comp = ['co', 'no', 'no2', 'o3', 'so2', 'pm2_5', 'pm10', 'nh3']

new_df_all = pd.DataFrame()

for i in list(cities_coordinates.keys()):
    # weather
    url_c = "https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_key}".format(
        lat=cities_coordinates[i]['lat'], lon=cities_coordinates[i]['lon'], API_key=api_auth_key)
    city = get(url_c)
    city = city.json()

    coord_data = json_to_df(city, search_coord_data, 'coord')
    sys_data = json_to_df(city, search_sys_data, 'sys')
    main_data = json_to_df(city, search_main_data, 'main')
    wind_data = json_to_df(city, search_wind_data, 'wind')
    cloud = json_to_df(city, search_cloud, 'clouds')
    visibility = df_creatio(city, 'visibility')
    base = df_creatio(city, 'base')
    dt = df_creatio(city, 'dt')
    timezone = df_creatio(city, 'timezone')
    name = df_creatio(city, 'name')
    cod = df_creatio(city, 'cod')
    new_df = pd.concat([coord_data, sys_data, main_data, wind_data, cloud, visibility, base, dt, timezone, name, cod],
                       axis=1)
    new_df['city'] = i
    new_df['date'] = now_dt
    new_df['temp_c'] = float(new_df['temp']) - 272.15
    new_df['feels_like_c'] = float(new_df['feels_like']) - 272.15
    new_df['temp_min_c'] = float(new_df['temp_min']) - 272.15
    new_df['temp_max_c'] = float(new_df['temp_max']) - 272.15
    time.sleep(2)

    # polution
    air_pol_url = "http://api.openweathermap.org/data/2.5/air_pollution/forecast?lat={lat}&lon={lon}&appid={API_key}".format(
        lat=cities_coordinates[i]['lat'], lon=cities_coordinates[i]['lon'], API_key=api_auth_key)
    polution_city = get(air_pol_url)
    polution_city = polution_city.json()

    polution_df = polution_city['list'][0]
    polution_comp_df = json_to_df(polution_df, pollution_comp, 'components')
    polution_comp_df['air_quality_v'] = str(polution_df['main']['aqi'])
    polution_comp_df['air_quality'] = polution_quality[str(polution_df['main']['aqi'])]
    # print("xx", polution_comp_df)

    new_df = pd.concat([new_df, polution_comp_df], axis=1)
    time.sleep(2)

    new_df_all = new_df_all.append(new_df)

new_df_all = hist_data.append(new_df_all)

# new_df_all.to_parquet("weather_hist_data" + '.parquet', engine='fastparquet')
#new_df_all.to_csv("weather_hist_data" + '.csv', encoding='utf-8', index=False)
print(new_df_all.tail())