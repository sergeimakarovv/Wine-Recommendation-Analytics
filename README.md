# `Wein für Sie`: Wine-Recommendation

![cover](https://github.com/yudinii/practice_/assets/157538170/723acb58-1667-4007-a4cc-0e6d396523fc)

[![Button-7.png](https://i.postimg.cc/mkDKKN93/Button-7.png)](https://public.tableau.com/app/profile/sergei.makarov/viz/WineDashboard_17067371597200/Main_Menu)

**`Wein für Sie`** provides detailed information about wineries, wines, their ratings, and food pairings. It is designed for wine enthusiasts and professionals seeking extensive wine-related data.

Google Cloud BigQuery is utilized for data storage and manipulation, and  Apache Airflow is implemented to create an end-to-end data pipeline.

## Why we made `Wein für Sie`?

![introduction](https://github.com/yudinii/practice_/assets/157538170/dfd1f3e3-b17d-440a-af42-c076cd581c08)

![why we made this](https://github.com/yudinii/practice_/assets/157538170/0c3e35b8-bc15-4276-bd55-f9274a59b8f3)

## Getting started

![api](https://github.com/yudinii/practice_/assets/157538170/aaa55c0b-6104-4a10-9372-b529e77851bd)

![api1](https://github.com/yudinii/practice_/assets/157538170/f11bdc92-3c9e-4835-b82e-6c80206be05d)

You can also copy the sample code below and read API implementation below:

#### 1) Get Wineries

-   Sample Request

```{=html}
<!-- -->
```
    GET https://apide.fly.dev/wineries

-   Sample Response Body

``` ruby
[
    {
        'website': 'http://www.vinicolaperini.com.br', 
        'wineryid': 10001, 
        'wineryname': 'Casa Perini'
    }
]
```

#### 2) Get Wine Information

-   Sample Request

```{=html}
<!-- -->
```
    GET https://apide.fly.dev/wine

-   Sample Response Body

``` ruby
[
    {
        'abv': 7.5, 
        'acidity': 'High', 
        'body': 'Medium-bodied',
        'code': 'BR', 
        'country': 'Brazil',
        'elaborate': 'Varietal/100%',
        'grapes': "['Muscat/Moscato']", 
        'harmonize': "['Pork', 'Rich Fish','Shellfish']",
        'regionid': 1001, 
        'regionname': 'Serra Gaúcha',
        'type': 'Sparkling', 
        'url': 'https://i.imgur.com/ff9UaLf.jpg', 
        'vintages': "[2020, 2019, 2018, 2017]", 
        'wineid': 100001, 
        'winename': 'Espumante Moscatel',
        'wineryid': 10001
    }
]
```

#### 3) Get Wine Ratings

-   Sample Request

```{=html}
<!-- -->
```
    GET https://apide.fly.dev/ratings

-   Sample Response Body

``` ruby
[
    {
        'avg_rating': 4.5, 
        'vintage': '1994',
        'wineid': 174184
    }
]
```

#### 4) Get Harmonizer Suggestion

-   Sample Request

```{=html}
<!-- -->
```
    GET https://apide.fly.dev/harmonizer

-   Sample Response Body

``` ruby
[
    {
        'snack': 'Aperitif',
        'url': 'https://i.imgur.com/wE2P1rn.jpg'
    }
]
```

# **API Implementation Documentation**

## **Overview**

This document details a Flask-based API interfacing with a PostgreSQL database, designed for managing data about wines, wineries, ratings, and harmonizer pictures. The deployment of both the Flask API and the PostgreSQL database is facilitated through **`fly.io`**, which simplifies the deployment process.

## **Database Schema**

![image](https://github.com/yudinii/practice_/assets/157538170/ce46d016-2367-41e6-ae6a-ae7179b4bdcd)

### **Database Setup**

### Create Database Tables

Tables are created using SQL **`CREATE TABLE`** statements stored in **`table_create_sql_dict`** and executed through the **`create_table`** function.

### Inserting Data

Individual data insertion functions (**`inser_winery_data`**, **`inser_wines_data`**, etc.) add data to each table, requiring tuples formatted according to the table schema.

### Bulk Data Insertion

**`bulk_inserting`** allows for inserting data in bulk from CSV files into the specified tables, matching the table's schema.

### Retrieving Data

**`get_data`** retrieves data from a table, returning it as a list of dictionaries where each dictionary represents a row.

### Listing All Tables

**`list_tables`** provides a list of all table names in the database.

## **API Endpoints**

### **`/wineries`, `/wine`, `/ratings`, `/harmonizer`**

Each endpoint corresponds to its respective table, offering GET methods to return JSON data about the entities in the database.

## **Running the Application**

Start the Docker container with **`docker-compose up`** to run the Flask application and Airflow scheduler. Access the API at **`localhost:8080`**.

## **Notes**

Ensure the correct setup of the PostgreSQL database, proper configuration of environment variables, and installation of dependencies as per **`requirements.txt`**. This API provides a structured and comprehensive approach to accessing and managing wine-related data.

![pipeline_edit](https://github.com/yudinii/practice_/assets/157538170/972ceeeb-fcf2-4eca-ba40-c77500034e6d)

    pip install -r requirements.txt

![pipeline1](https://github.com/yudinii/practice_/assets/157538170/ae878010-4c8c-4677-891c-2b9e58da12bb)

![pipeline2](https://github.com/yudinii/practice_/assets/157538170/b37bd870-816a-4ba5-8d81-021934ea6bab)

![pipeline3](https://github.com/yudinii/practice_/assets/157538170/7355970c-b9a5-4506-b197-24fdf96252d8)

![pulling](https://github.com/yudinii/practice_/assets/157538170/4bfd077f-d399-4029-b1a9-e1e4b3a241fa)

![pulling1](https://github.com/yudinii/practice_/assets/157538170/9a052ae9-e0ff-4ee2-bc28-46a87ac6f70b)

![pulling2](https://github.com/yudinii/practice_/assets/157538170/9b68e316-a6b0-4305-8153-5ad17bdadb57)

![bigquery](https://github.com/yudinii/practice_/assets/157538170/4bab7099-35ad-4f8a-b0f1-f64bc1e9a9fd)

![bigquery1](https://github.com/yudinii/practice_/assets/157538170/bfe1a5fb-e6c6-4fff-a868-563d76991885)

![bigquery2](https://github.com/yudinii/practice_/assets/157538170/34d86ac1-bb9e-4e68-b8ad-fbff099dd3fb)

![bigquery3](https://github.com/yudinii/practice_/assets/157538170/2127b218-f45b-4c58-93a7-6eb220324c72)

![Dashboard](https://github.com/yudinii/practice_/assets/157538170/7e63cd4a-c548-4579-b6b5-5154f44ed0b9)

![Dashboard1](https://github.com/yudinii/practice_/assets/157538170/643a895f-6482-4a7d-9209-f658858322b3)

![Dashboard2](https://github.com/yudinii/practice_/assets/157538170/c0d891ba-8b28-4f20-ab5c-fd1aeb7e9b02)

