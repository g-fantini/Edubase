'''
Created on 17 Jul 2020

@author: Gabriele Fantini
'''
import pandas as pd 
import sqlite3
from string import Template

if __name__ == '__main__':
  
    def create_connection(db_file):
        """Creates a database connection to the SQLite database
            specified by the db_file
        :param db_file: database file
        :return: Connection object or None
        """
        conn = None
        try:
            conn = sqlite3.connect(db_file)
        except Exception as e:
            print(e)
    
        return conn
    
    def query(conn, query):
        """
        Executes the given query
        :param conn: the Connection object
        :return:
        Array containing the query result
        """
        cur = conn.cursor()
        cur.execute(query)
    
        rows = cur.fetchall()
        return rows
    
    def parse_csv(file_name):
        """
        Parses the passed csv_file and creates a dataframe object 
        :param file_name: (str) file to parse
        :return:
        Dataframe object
        """
        parse_dates = ['opendate','closedate']
        return pd.read_csv(file_name, low_memory=False, parse_dates=parse_dates)
    
    
    #Q1_How many open primary schools are there in Tower Hamlets?
    def get_shools_by_location_level_status(la_name, level="'Primary'", status="'%Open%'" ):
        """
        Query {status} {level} schools in a specific location
        :param la_name: (str) location
        :param level: (str) education level
        :param status: (str) status (Open,Close)
        :return:
        Array of results
        """
        
        #Uses the LIKE for establishmentstatus_name, in order to include any school with "Open, but proposed to close" status
        query_template = Template('''SELECT count(*)
                        FROM Schools 
                        WHERE la_name = $la_name
                        and phaseofeducation_name = $level 
                        and establishmentstatus_name LIKE $status
                    ''')
        return query(conn,  query_template.substitute(la_name=la_name, level=level, status=status))[0][0]
    
    #2. Give a list of the districts (districtadministrative_name) that have had the most schools close since 2000
    def get_top_closed_districts(closedate, limit = 10):
        """
        Query districts that have had the most schools close since {date}
        :param closedate: (str) school close date
        :param limit: (str) limit for the query
        :return:
        Array of results
        """
        query_template = Template('''SELECT count(urn) as 'occurrence', districtadministrative_name
                                   FROM Schools 
                                   WHERE closedate >= $closedate
                                   GROUP BY districtadministrative_name
                                   ORDER BY occurrence DESC
                                   LIMIT $limit
                                ''')
        return query(conn,  query_template.substitute(closedate=closedate, limit=limit))
    
    #Calculate the percentage breakdown of open schools in each Local Authority(la_name). Which LAs form the lowest 10 percentages?
    def get_open_schools_breakdown_by_location(limit):
        """
        Calculates the percentage breakdown of open schools in each Local Authority(la_name)and returns the 10%
        :param closedate: (str) school close date
        :param limit: (str) limit for the query
        :return:
        Array of results
        """
        query_template = Template('''SELECT m.la_name, (m.open_counter *100/ l.total_s )  as perc
                                    FROM
                                        (SELECT count(*) as total_s, la_name
                                                                FROM Schools 
                                                                GROUP BY la_name ) AS l JOIN
                                        (SELECT count(*) as open_counter, la_name
                                                                FROM Schools 
                                                                WHERE establishmentstatus_name = 'Open'  
                                                                GROUP BY la_name ) AS m 
                                    ON l.la_name = m.la_name
                                    
                                    ORDER BY perc 
                                    LIMIT $limit
                                ''')
        return query(conn,  query_template.substitute(limit=limit))
    
    #parse the csv file
    data = parse_csv("edubase_data.csv")
    
    #create a database connection 
    conn = create_connection('Edubase.db')
    
    #Loads the dataframe to the School table in the database, replace the table if already exists
    data.to_sql("Schools", conn, if_exists='replace')
    
    print("How many open primary schools are there in Tower Hamlets? \n" + str(get_shools_by_location_level_status("'Tower Hamlets'")))
    
    print("Give a list of the districts (districtadministrative_name) that have had the most schools close since 2000: \n" + str(get_top_closed_districts("'2000-01-01'")))
    
    #Get the number of districts to facilitate 3rd query
    total_of_dinstricts = (query(conn, "SELECT count(DISTINCT(la_name)) FROM Schools "))[0][0]
    
    print("Calculate the percentage breakdown of open schools in each Local Authority(la_name). Which LAs form the lowest 10 percentages? \n" + str(get_open_schools_breakdown_by_location(str(int(round(total_of_dinstricts/10)))))) 
    