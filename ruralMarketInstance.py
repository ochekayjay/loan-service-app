



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
    
        print(element)
        try:
            # Connect to the PostgreSQL database
            print('Connecting to the PostgreSQL database...')
        
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
            print('PostgreSQL database version:')
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
            if conn is not None:
                #conn.close()
                print('Database connection closed.')
    
    cur.close()
    conn.close()








def createRuralInfoTable():

    import psycopg2
    import time
    import random
    import pandas as pd

    db_params = {
        "host"    : "localhost",
        "database": "ruleToData",
        "user"    : "ruleToData",
        "password": "rule2Data",
        "port"    : "5432"  # Default PostgreSQL port
    }
    
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    #assessment = ['pending','granted','rejected']
    assessmentList = ['pending']
    target_amount = ['100000-300000','300000-500000','500000-800000']
    payment_completion_timeline = ['3','5','7','9','12','24']


    try:
        # Connect to the PostgreSQL database
        print('Connecting to the PostgreSQL database...')
        
        
        # Execute a simple query to verify the connection
        cur.execute("""
            CREATE TABLE IF NOT EXISTS loan_requests (id SERIAL PRIMARY KEY ,
                                                    name TEXT,
                                                    profileid INTEGER ,
                                                    age TEXT, 
                                                    assessment TEXT, 
                                                    target_amount TEXT, 
                                                    payment_completion_timeline TEXT,
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
                                                    gender TEXT,
                                                    review_round TEXT)
        """)
        
        conn.commit()
        
        # Close the cursor and connection
        print('Table gets created')
        #cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"An error occurred: {error}")

    finally:
        if conn is not None:
            #conn.close()
            print('Database connection closed.')

 






def firstInsertRuralInserts():

    import psycopg2
    import time
    import random
    import pandas as pd

    db_params = {
        "host"    : "localhost",
        "database": "ruleToData",
        "user"    : "ruleToData",
        "password": "rule2Data",
        "port"    : "5432"  # Default PostgreSQL port
    }

    assessmentList = ['pending']
    target_amount = ['100000-300000','300000-500000','500000-800000']
    payment_completion_timeline = ['3','5','7','9','12','24']
    review_round = 'first'

    conn = psycopg2.connect(**db_params) 
        
        # Create a cursor object
    cur = conn.cursor() 

    cur = conn.cursor()
    cur.execute("SELECT * FROM marketers_borrower_profile_three")
    rows = cur.fetchall()

    colnames = [desc[0] for desc in cur.description]

    df = pd.DataFrame(rows, columns=colnames)

    detail_brkdown = [(element[0],element[1]) for element in rows]

    for index,element in df.iterrows():
    

        try:
            # Connect to the PostgreSQL database
            print('Connecting to the PostgreSQL database for rural marketers...')
            assessment = random.choice(assessmentList)
            target = random.choice(target_amount)
            completion_timeline = random.choice(payment_completion_timeline)
            
            
            # Execute a simple query to verify the connection
            print('PostgreSQL database version:')
            cur.execute("""
            INSERT INTO loan_requests (name,profileid,age,
                                    assessment,target_amount,
                                    payment_completion_timeline,
                                    no_of_kids, marital_status,
                                    no_of_cars ,no_of_houses,
                                    residential_location ,occupation_type,average_monthly_income,
                                    average_monthly_spend,business_valuation,number_of_staffs,mobility_index,gender,review_round) 
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            ,(element['name'],element['id'],element['age'],assessment,target,completion_timeline,
                element['no_of_kids'],element['marital_status'],element['no_of_cars'],element['no_of_houses'],
                element['residential_location'],element['occupation_type'],element['average_monthly_income']
                ,element['average_monthly_spend'],element['business_valuation'],element['number_of_staffs'],element['mobility_index'],element['gender'],review_round
            ))

            
            conn.commit()
            #print(f'{element} inserted successfully for rural marketers.')
            
            # Close the cursor and connection
            #cur.close()

        except (Exception, psycopg2.DatabaseError) as error:
            print(f"An error occurred: {error}")

        finally:
            if conn is not None:
                #conn.close()
                print('Dataloop loop of First loopers.')
    cur.close()
    conn.close()






def MultipleInsertRuralInserts():

    import psycopg2
    import time
    import random
    import pandas as pd

    db_params = {
        "host"    : "localhost",
        "database": "ruleToData",
        "user"    : "ruleToData",
        "password": "rule2Data",
        "port"    : "5432"  # Default PostgreSQL port
    }

    assessmentList = ['pending']
    target_amount = ['100000-300000','300000-500000','500000-800000']
    payment_completion_timeline = ['3','5','7','9','12','24']
    review_round = 'first'

    conn = psycopg2.connect(**db_params) 
        
        # Create a cursor object
    cur = conn.cursor() 

    cur = conn.cursor()

    while True:
        cur.execute("SELECT * FROM marketers_borrower_profile")
        rows = cur.fetchall()

        colnames = [desc[0] for desc in cur.description]

        df = pd.DataFrame(rows, columns=colnames)

        detail_brkdown = [(element[0],element[1]) for element in rows]

        for index,element in df.iterrows():
        

            try:
                # Connect to the PostgreSQL database
                print('Connecting to the PostgreSQL database for rural marketers...')
                assessment = random.choice(assessmentList)
                target = random.choice(target_amount)
                completion_timeline = random.choice(payment_completion_timeline)
                
                
                # Execute a simple query to verify the connection
                print('PostgreSQL database version:')
                cur.execute("""
                INSERT INTO loan_requests (name,profileid,age,
                                        assessment,target_amount,
                                        payment_completion_timeline,
                                        no_of_kids, marital_status,
                                        no_of_cars ,no_of_houses,
                                        residential_location ,occupation_type,average_monthly_income,
                                        average_monthly_spend,business_valuation,number_of_staffs,mobility_index,gender,review_round) 
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                ,(element['name'],element['id'],element['age'],assessment,target,completion_timeline,
                    element['no_of_kids'],element['marital_status'],element['no_of_cars'],element['no_of_houses'],
                    element['residential_location'],element['occupation_type'],element['average_monthly_income']
                    ,element['average_monthly_spend'],element['business_valuation'],element['number_of_staffs'],element['mobility_index'],element['gender'],review_round
                ))

                
                conn.commit()
                print('inserted successfully for rural marketers.')
                
                # Close the cursor and connection
                #cur.close()

            except (Exception, psycopg2.DatabaseError) as error:
                print(f"An error occurred: {error}")

            finally:
                if conn is not None:
                    #conn.close()
                    print('inserted successfully for rural marketers 2.')

            time.sleep(75)  
    cur.close()
    conn.close()




"""
def updateNewInserts():   
    
    import psycopg2
    import time
    import random
    import pandas as pd


    db_params = {
        "host"    : "localhost",
        "database": "ruleToData",
        "user"    : "ruleToData",
        "password": "rule2Data",
        "port"    : "5432"  # Default PostgreSQL port
    }
    
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()


    cur.execute( SELECT * FROM loan_requests WHERE ( occupation_type ='business_owner' 
        AND residential_location = 'low-cost housing')  
        OR ( occupation_type ='business_owner' 
        AND residential_location = 'government housing') 
        )
    
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    newDf = pd.DataFrame(rows,columns=columns)

    while 'jay':
        #conn = None
        try:
            index = random.choice( list(range(len(newDf))))

            target_amount_list = ['100000-300000','300000-500000','500000-800000','800000-1000000','1000000+']
            payment_completion_timeline_list = ['3','5','7','9','12','24']

            target_amount = 'target_amount'
            payment_completion_timeline = 'payment_completion_timeline'

            choice = [target_amount,payment_completion_timeline]
            randData = random.choice(choice)


            if randData == target_amount:
                new_targ_amt = target_amount_list.copy()
                print(newDf[randData][index])
                new_targ_amt.remove(newDf[randData][index])
                rand_selected = random.choice(new_targ_amt)

                cur.execute(
                        UPDATE loan_requests
                        SET target_amount = %s
                        WHERE id = %s
                    , (rand_selected, index))
                    
                    
                conn.commit()
                print('target_amount committed successfully.')
            else:
                new_pay_completion_tl = payment_completion_timeline_list.copy()
                print(newDf[randData][index])
                new_pay_completion_tl.remove(newDf[randData][index])
                rand_selected = random.choice(new_pay_completion_tl)

                cur.execute(
                        UPDATE loan_requests
                        SET payment_completion_timeline = %s
                        WHERE id = %s
                    , (rand_selected, index))
                    
                    
                conn.commit()
                print('from rural market 1')
                print('payment_completion_timeline committed successfully.')
            # Connect to the PostgreSQL database
        
            
            # Close the cursor and connection
            #cur.close()

        except (Exception, psycopg2.DatabaseError) as error:
            print(f"An error occurred: {error}")

        finally:
            if conn is not None:
                #conn.close()
                print('Database connection closed.')
        print('from rural market 2')
        time.sleep(13)

 


    """

"""
    conn = psycopg2.connect(**db_params) 
        
        # Create a cursor object
    cur = conn.cursor() 

    cur = conn.cursor()
    cur.execute("SELECT * FROM borrower_profile")
    rows = cur.fetchall()

    colnames = [desc[0] for desc in cur.description]

    df = pd.DataFrame(rows, columns=colnames)

    detail_brkdown = [(element[0],element[1]) for element in rows]

    for index,element in df.iterrows():
    

        try:
            # Connect to the PostgreSQL database
            print('Connecting to the PostgreSQL database...')
            assessment = random.choice(assessmentList)
            target = random.choice(target_amount)
            completion_timeline = random.choice(payment_completion_timeline)
            
            
            # Execute a simple query to verify the connection
            print('PostgreSQL database version:')
            cur.execute(
            INSERT INTO loan_requests_two (name,profileid,age,
                                    assessment,target_amount,
                                    payment_completion_timeline,
                                    no_of_kids, marital_status,
                                    no_of_cars ,no_of_houses,
                                    residential_location ,occupation_type,average_monthly_income,
                                    average_monthly_spend,business_valuation,number_of_staffs,mobility_index,gender) 
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ,(element['name'],element['id'],element['age'],assessment,target,completion_timeline,
                element['no_of_kids'],element['marital_status'],element['no_of_cars'],element['no_of_houses'],
                element['residential_location'],element['occupation_type'],element['average_monthly_income']
                ,element['average_monthly_spend'],element['business_valuation'],element['number_of_staffs'],element['mobility_index'],element['gender']
            ))

            
            conn.commit()
            print(f'{element} inserted successfully.')
            
            # Close the cursor and connection
            #cur.close()

        except (Exception, psycopg2.DatabaseError) as error:
            print(f"An error occurred: {error}")

        finally:
            if conn is not None:
                #conn.close()
                print('Database connection closed.')
    cur.close()
    conn.close()
    """


def completeCall():
    #populateRuralProfile()

    createRuralInfoTable()

    firstInsertRuralInserts()

    MultipleInsertRuralInserts()

    #createRuralInfoTable()

    #insertRuralInserts()

    #updateNewInserts()

"""

        

"""

