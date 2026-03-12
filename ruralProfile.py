



def populateRuralProfile():

    import psycopg2
    import time
    import random
    import pandas as pd


    names = [
         "Hassan",
        "Audu",
        "Folakemi",
        "Mistura",
        "Jonah",


        "Oche",
        "Arome",
        "Zamani",
        "Blessing",
        "Habiba",
        "Efetobore"

        

        "Nneka",
        "Oyifioda",
        "Edache",
        "Chidera",
        "Kabiru",
        "Sunmisola",
        "Adaobi"
    
    ]


    db_params = {
        "host"    : "localhost",
        "database": "ruleToData",
        "user"    : "ruleToData",
        "password": "rule2Data",
        "port"    : "5432"  # Default PostgreSQL port
    }

    age = []
    no_of_kids = []
    occupation_type_list = ['business_owner']
    lower_thirty_occupations = ['business_owner','salary_earner','student','student & business owner']
    marital_status_list = ['single','married','divorced']
    no_of_cars = []
    no_of_houses = []
    lower_thirty_residential_list = ['low-cost housing']
    residential_location_list = ['low-cost housing','government housing']
    gender_list = ['female','male']
    business_valuation = ''
    number_of_staffs = ''
    mobility_index = ''


    below_thirty_income = list(range(70000, 1300000, 3400))

    lower_thirty = [v for v in below_thirty_income if v < 150000]
    middle_lower_thirty = [v for v in below_thirty_income if v >= 150000 and v<450000]
    upper_lower_thirty = [v for v in below_thirty_income if v >= 450000]

    lower_spending = list(range(50000, 70000, 3150))
    middle_spending = list(range(75000, 135000, 6000))
    upper_spending = list(range(136000, 435000, 10250))




    above_thirty_income = list(range(150000, 300000, 2430))

    lower_income = [v for v in above_thirty_income if v < 200000]
    middle_income = [v for v in above_thirty_income if v >= 210000 and v<250000]
    upper_income = [v for v in above_thirty_income if v >= 250000]

    lower_spending_above = list(range(140000, 150000, 1350))
    middle_spending_above = list(range(150000, 210000, 3300))
    upper_spending_above = list(range(210000, 250000, 2250))






    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    try:
        # Connect to the PostgreSQL database
        print('Connecting to the PostgreSQL database...')
        
        
        # Create a cursor object
        
        
        # Execute a simple query to verify the connection
        cur.execute("""CREATE TABLE IF NOT EXISTS marketers_borrower_profile (id SERIAL PRIMARY KEY ,
                                                                    name TEXT, 
                                                                    age INTEGER, 
                                                                    no_of_kids INTEGER, 
                                                                    marital_status TEXT,
                                                                    no_of_cars INTEGER,
                                                                    no_of_houses INTEGER,
                                                                    residential_location TEXT,
                                                                    occupation_type TEXT,
                                                                    average_monthly_income INTEGER,
                                                                    average_monthly_spend INTEGER,
                                                                    business_valuation INTEGER,
                                                                    number_of_staffs INTEGER,
                                                                    mobility_index INTEGER,
                                                                    gender TEXT)
        
        """)
        
        conn.commit()
        
        # Close the cursor and connection
        print('Marketers Table gets created')
       

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"An error occurred: {error}")

    finally:
        if conn is not None:
            #conn.close()
            print('Database connection closed.')





    #conn = psycopg2.connect(**db_params) 
        
        # Create a cursor object
    #cur = conn.cursor() 
    for element in names:
    
        #print(element)
        try:
            # Connect to the PostgreSQL database
            #print('Connecting to the PostgreSQL database...')
        
            name = element
            age = random.choice(list(range(30,54,4))) 
        
            no_of_kids = random.choice(list(range(0,8,1)))
            marital_status = random.choice(['single','married','divorced','widow','widower'])
            no_of_cars = random.choice(list(range(0,8,1))) 
            no_of_houses = random.choice(list(range(0,8,1))) ,

            residential_location = random.choice(residential_location_list)

            gender = random.choice(gender_list)

            occupation_type = ''
            business_valuation = 0
            number_of_staffs = 0 
            mobility_index = 0


            occupation_type = random.choice(occupation_type_list)
            business_valuation = random.choice(list(range(200000,2000000,13400)))
            number_of_staffs = random.choice(list(range(0,9,1)))
            mobility_index = random.choice(list(range(0,20,3)))

            average_monthly_income = 0
            average_monthly_spend = 0
            randValue = random.random()

            if randValue <= 0.75 and randValue > 0.35:
                average_monthly_income = random.choice(middle_income)
                average_monthly_spend = random.choice(middle_spending_above)
            elif randValue <= 0.35:
                average_monthly_income = random.choice(lower_income)
                average_monthly_spend = random.choice(lower_spending_above)

            else :
                average_monthly_income = random.choice(upper_income)
                average_monthly_spend = random.choice(upper_spending_above)
            
            
            # Execute a simple query to verify the connection
            #print('PostgreSQL database version:')
            cur.execute("""
            INSERT INTO marketers_borrower_profile (name,age,no_of_kids,marital_status,no_of_cars,no_of_houses,residential_location,occupation_type,average_monthly_income,
                                                                    average_monthly_spend,business_valuation,number_of_staffs,mobility_index,gender) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,(element,age,no_of_kids,marital_status,no_of_cars,no_of_houses,residential_location,occupation_type,average_monthly_income,average_monthly_spend,business_valuation,number_of_staffs,mobility_index,gender))
            
            
            conn.commit()
            print(f'{element} inserted successfully.')
            
            # Close the cursor and connection
            #cur.close()

        except (Exception, psycopg2.DatabaseError) as error:
            print(f"An error occurred: {error}")

        finally:
            #if conn is not None:
                #conn.close()
                print('Database connection closed.')
    
    cur.close()
    conn.close()



def ruralProfileTimelyUpdate():

    import psycopg2
    import time
    import random
    import pandas as pd


    names = [
         "Hassan",
        "Audu",
        "Folakemi",
        "Mistura",
        "Jonah",


        "Oche",
        "Arome",
        "Zamani",
        "Blessing",
        "Habiba",
        "Efetobore"

        

        "Nneka",
        "Oyifioda",
        "Edache",
        "Chidera",
        "Kabiru",
        "Sunmisola",
        "Adaobi"
    
    ]


    db_params = {
        "host"    : "localhost",
        "database": "ruleToData",
        "user"    : "ruleToData",
        "password": "rule2Data",
        "port"    : "5432"  # Default PostgreSQL port
    }

    conn = psycopg2.connect(**db_params)      
    # Create a cursor object
    cur = conn.cursor()
    

    below_thirty_income = list(range(70000, 1300000, 3400))

    lower_thirty = [v for v in below_thirty_income if v < 150000]
    middle_lower_thirty = [v for v in below_thirty_income if v >= 150000 and v<450000]
    upper_lower_thirty = [v for v in below_thirty_income if v >= 450000]

    lower_spending = list(range(50000, 70000, 3150))
    middle_spending = list(range(75000, 135000, 6000))
    upper_spending = list(range(136000, 435000, 10250))




    above_thirty_income = list(range(150000, 300000, 2430))

    lower_income = [v for v in above_thirty_income if v < 200000]
    middle_income = [v for v in above_thirty_income if v >= 210000 and v<250000]
    upper_income = [v for v in above_thirty_income if v >= 250000]

    lower_spending_above = list(range(140000, 150000, 1350))
    middle_spending_above = list(range(150000, 210000, 3300))
    upper_spending_above = list(range(210000, 250000, 2250))

    while 'jay':
    #conn = None
        cur.execute("SELECT * FROM marketers_borrower_profile")
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        newDf = pd.DataFrame(rows,columns=columns)
        try:
            index = random.choice( list(range(len(newDf))))
            age = newDf['age'][index]

            no_of_cars = random.choice(list(range(0,8,1))) 
            no_of_houses = random.choice(list(range(0,8,1))) 
            randValue = random.random()

            if randValue <= 0.75 and randValue > 0.35:
                average_monthly_income = random.choice(middle_income)
                average_monthly_spend = random.choice(middle_spending_above)
            elif randValue <= 0.35:
                average_monthly_income = random.choice(lower_income)
                average_monthly_spend = random.choice(lower_spending_above)

            else :
                average_monthly_income = random.choice(upper_income)
                average_monthly_spend = random.choice(upper_spending_above)


            query = f"""
                    UPDATE marketers_borrower_profile
                    SET no_of_cars = %s,
                        no_of_houses = %s,
                        average_monthly_income = %s,
                        average_monthly_spend = %s
                    WHERE id = %s
                """

            cur.execute(query, (no_of_cars,no_of_houses,average_monthly_income,average_monthly_spend,index))
            conn.commit()
            print(f'adjusted {average_monthly_income} and {no_of_houses}')

        except (Exception, psycopg2.DatabaseError) as error:
                print(f"An error occurred: {error}")

        finally:
                if conn is not None:
                    #conn.close()
                    print('Database connection closed.')  

        time.sleep(30)     

def baseFunc():
    populateRuralProfile()
    ruralProfileTimelyUpdate()

baseFunc()
