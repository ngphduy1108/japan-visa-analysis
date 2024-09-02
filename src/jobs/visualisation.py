from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

import pandas as pd
from fuzzywuzzy import process    # type: ignore
import pycountry
import pycountry_convert as pcc   # type: ignore
import plotly.express as px   # type: ignore


# Create a Spark session
spark = SparkSession.builder.appName("Visa Application Japan").getOrCreate()

df = spark.read.csv('D:\Duy\hoc tap\Portfolio\Project\japan-visa-analysis\src\input',
                    header=True,
                    inferSchema=True)

# Replace spaces, slashes, dots, and commas in column names
new_col_names = [col.replace(' ', '_')
                    .replace('/', '')
                    .replace('.', '')
                    .replace(',', '') for col in df.columns]
df = df.toDF(*new_col_names)

# Drop all columns with null values
df = df.dropna(how='all')

# Get the necessary columns
df = df.select('year', 'country', 'number_of_issued_numerical')


# Correct the country names
def correct_country_name(country_name, threshold=85):
    countries = [country.name for country in pycountry.countries]
    correct_name, score = process.extractOne(country_name, countries)
    if score >= threshold:
        return correct_name
    # No match found, keep the original name
    return country_name


# Get the continent name
def get_continent_name(country_name):
    try:
        country_code = pcc.country_name_to_country_alpha2(country_name, cn_name_format="default")  # noqa
        continent_code = pcc.country_alpha2_to_continent_code(country_code)
        return pcc.convert_continent_code_to_continent_name(continent_code)
    except: # noqa
        return None


correct_country_names_udf = udf(correct_country_name, StringType())
df = df.withColumn('country', correct_country_names_udf(df['country']))

country_corrections = {
    'Andra': 'Russia',
    'Antigua Berbuda': 'Antigua and Barbuda',
    'Barrane': 'Bahrain',
    'Brush': 'Bhutan',
    'Komoro': 'Comoros',
    'Benan': 'Benin',
    'Kiribass': 'Kiribati',
    'Gaiana': 'Guyana',
    'Court Jiboire': "Côte d'Ivoire",
    'Lesot': 'Lesotho',
    'Macau travel certificate': 'Macao',
    'Moldoba': 'Moldova',
    'Naure': 'Nauru',
    'Nigail': 'Niger',
    'Palao': 'Palau',
    'St. Christopher Navis': 'Saint Kitts and Nevis',
    'Santa Principa': 'Sao Tome and Principe',
    'Saechel': 'Seychelles',
    'Slinum': 'Saint Helena',
    'Swaji Land': 'Eswatini',
    'Torque menistan': 'Turkmenistan',
    'Tsubaru': 'Zimbabwe',
    'Kosovo': 'Kosovo'
}
df = df.replace(country_corrections, subset='country')   # type: ignore

continent_udf = udf(get_continent_name, StringType())
df = df.withColumn('continent', continent_udf(df['country']))

df.createGlobalTempView('visa_applications')

# Visualisation

# The number of visa applications in Japan between 2006 and 2017
df_cont = spark.sql('''
                    SELECT year, continent,
                        SUM(number_of_issued_numerical) AS applications
                    FROM global_temp.visa_applications
                    WHERE continent IS NOT NULL
                    GROUP BY year, continent
                    ''')

df_cont = df_cont.toPandas()

fig = px.bar(df_cont, x='year', y='applications', color='continent', barmode='group')   # noqa
fig.update_layout(title_text='Total Visa Applications in Japan between 2006 and 2017',   # noqa
                  xaxis_title='Year', yaxis_title='Total Visa Applications',
                  legend_title='Continent')
fig.write_html('output/visa_number_japan_continent_2006_2017.html')

# The top 10 countries having the most visa applications in 2017
df_10_countries = spark.sql('''
                    SELECT country,
                        SUM(number_of_issued_numerical) AS applications
                    FROM global_temp.visa_applications
                    WHERE country NOT IN ('total', 'others')
                        AND country IS NOT NULL
                        AND year = 2017
                    GROUP BY country
                    ORDER BY applications DESC
                    LIMIT 10
                    ''')

df_10_countries = df_10_countries.toPandas()

fig = px.bar(df_10_countries, x='country', y='applications', color='country')
fig.update_layout(title_text='Top 10 Countries having the most Visa Applications in 2017',   # noqa
                  xaxis_title='Country', yaxis_title='Total Visa Applications',
                  legend_title='Country')
fig.write_html('output/visa_number_japan_top_10_2017.html')

# Show the output on the map
df_year_countries = spark.sql('''
                    SELECT year, country,
                        SUM(number_of_issued_numerical) AS applications
                    FROM global_temp.visa_applications
                    WHERE country NOT IN ('total', 'others')
                        AND country IS NOT NULL
                    GROUP BY year, country
                    ORDER BY year ASC
                    ''')
df_year_countries = df_year_countries.toPandas()

fig = px.choropleth(df_year_countries, locations='country',
                    color='applications', hover_name='country',
                    animation_frame='year',
                    range_color=[100000, 100000],
                    color_continuous_scale=px.colors.sequential.Plasma,
                    locationmode='country names',
                    title='Visa Applications by Countries over time')
fig.write_html('output/visa_number_japan_country_map.html')

# Write the output to a file
df.write.csv('output/visa_number_in_japan_cleaned.csv', header=True, mode='overwrite')   # noqa


spark.stop()
