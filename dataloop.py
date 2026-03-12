





def createLoanReqTable():

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

    #names = ['ola','Joe','bibi','Ikota','Damo','ebuka','Tosin','Igoche']
    names = [
        "Chinedu",
        "Oluwaseun",
        "Ifeanyi",
        "Aisha",
        "Tobiloba",
        "Emeka",
        "Amarachi",
        "Sadiq",
        "Folake",
        "Ebuka",

        "Zainab",
        "Uche",
        "Temitope",
        "Nnamdi",
        "Hadiza",
        "Toluwani",
        "Efe",
        "Ngozi",
        "Ayomide",
        "Yakubu",

        "Chisom",
        "Kehinde",
        "Mariam",
        "Obinna",
        "Yetunde",
        "Favour",
        "Bashir",
        "Somto",
        "Kelechi",
        "Halima",

        "Bolanle",
        "Olamide",
        "Ireti",
        "Chukwudi",
        "Hauwa",
        "Seyi",
        "Tobechukwu",
        "Binta",
        "Damilola",
        "Uzoma",

        "Nneka",
        "Ridwan",
        "Funmilayo",
        "Chidera",
        "Kabiru",
        "Simisola",
        "Olumide",
        "Adaobi",
        "Mustapha",
        "Obianuju"
    ]

    #assessment = ['pending','granted','rejected']
    assessmentList = ['pending']
    target_amount = ['100000-300000','300000-500000','500000-800000','800000-1000000','1000000+']
    payment_completion_timeline = ['3','5','7','9','12','24']

    conn = None

    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_params)
        
        # Create a cursor object
        cur = conn.cursor()
        
        # Execute a simple query to verify the connection
        cur.execute("""
            CREATE TABLE IF NOT EXISTS loan_requests_three (id SERIAL PRIMARY KEY ,
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
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"An error occurred: {error}")

    finally:
        if conn is not None:
            conn.close()
            print('Database connection dataloop closed.')





def firstInsertInReqTable():

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
    target_amount = ['100000-300000','300000-500000','500000-800000','800000-1000000','1000000+']
    payment_completion_timeline = ['3','5','7','9','12','24']
    review_round = 'first'

    conn = psycopg2.connect(**db_params) 
        
        # Create a cursor object
    cur = conn.cursor() 

    
    
    cur.execute("SELECT * FROM borrower_profile_three")
    rows = cur.fetchall()

    colnames = [desc[0] for desc in cur.description]

    df = pd.DataFrame(rows, columns=colnames)

    detail_brkdown = [(element[0],element[1]) for element in rows]

    for index,element in df.iterrows():
    

        try:
            # Connect to the PostgreSQL database
            assessment = random.choice(assessmentList)
            target = random.choice(target_amount)
            completion_timeline = random.choice(payment_completion_timeline)
            
            
            # Execute a simple query to verify the connection
            cur.execute("""
            INSERT INTO loan_requests_three (name,profileid,age,
                                    assessment,target_amount,
                                    payment_completion_timeline,
                                    no_of_kids, marital_status,
                                    no_of_cars ,no_of_houses,
                                    residential_location ,occupation_type,average_monthly_income,
                                    average_monthly_spend,business_valuation,number_of_staffs,mobility_index,gender,review_round) 
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            ,
            (element['name'],element['id'],element['age'],assessment,target,completion_timeline,
                element['no_of_kids'],element['marital_status'],element['no_of_cars'],element['no_of_houses'],
                element['residential_location'],element['occupation_type'],element['average_monthly_income']
                ,element['average_monthly_spend'],element['business_valuation'],element['number_of_staffs'],element['mobility_index'],element['gender'],review_round
            ))

            
            conn.commit()
            
            # Close the cursor and connection
            #cur.close()

        except (Exception, psycopg2.DatabaseError) as error:
            print(f"An error occurred: {error}")

        finally:
            if conn is not None:
                #conn.close()
                print('Dataloop loop of First Inserts.')
    

    cur.close()
    conn.close()
    


def secondInsertInReqTable():

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
    target_amount = ['100000-300000','300000-500000','500000-800000','800000-1000000','1000000+']
    payment_completion_timeline = ['3','5','7','9','12','24']
    review_round = 'first'

    conn = psycopg2.connect(**db_params) 
        
        # Create a cursor object
    cur = conn.cursor() 

    
    while True:
        cur.execute("SELECT * FROM borrower_profile_three")
        rows = cur.fetchall()

        colnames = [desc[0] for desc in cur.description]

        df = pd.DataFrame(rows, columns=colnames)

        detail_brkdown = [(element[0],element[1]) for element in rows]

        for index,element in df.iterrows():
        

            try:
                # Connect to the PostgreSQL database
                assessment = random.choice(assessmentList)
                target = random.choice(target_amount)
                completion_timeline = random.choice(payment_completion_timeline)
                
                
                # Execute a simple query to verify the connection
                cur.execute("""
                INSERT INTO loan_requests_three (name,profileid,age,
                                        assessment,target_amount,
                                        payment_completion_timeline,
                                        no_of_kids, marital_status,
                                        no_of_cars ,no_of_houses,
                                        residential_location ,occupation_type,average_monthly_income,
                                        average_monthly_spend,business_valuation,number_of_staffs,mobility_index,gender,review_round) 
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                ,
                (element['name'],element['id'],element['age'],assessment,target,completion_timeline,
                    element['no_of_kids'],element['marital_status'],element['no_of_cars'],element['no_of_houses'],
                    element['residential_location'],element['occupation_type'],element['average_monthly_income']
                    ,element['average_monthly_spend'],element['business_valuation'],element['number_of_staffs'],element['mobility_index'],element['gender'],review_round
                ))

                
                conn.commit()
                print('inserted successfully for loopers 1.')
                
                # Close the cursor and connection
                #cur.close()

            except (Exception, psycopg2.DatabaseError) as error:
                print(f"An error occurred: {error}")

            finally:
                if conn is not None:
                    #conn.close()
                    print('inserted successfully for loopers 2.')
            time.sleep(30)

    cur.close()
    conn.close()





def updateReqTable():

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
    # Create a cursor object
    cur = conn.cursor()
    cur.execute("SELECT * FROM loan_requests_three")
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

                cur.execute("""
                        UPDATE loan_requests_three
                        SET target_amount = %s
                        WHERE id = %s
                    """, (rand_selected, index))
                    
                    
                conn.commit()
                print('target_amount committed successfully.')
            else:
                new_pay_completion_tl = payment_completion_timeline_list.copy()
                print(newDf[randData][index])
                new_pay_completion_tl.remove(newDf[randData][index])
                rand_selected = random.choice(new_pay_completion_tl)

                cur.execute("""
                        UPDATE loan_requests_three
                        SET payment_completion_timeline = %s
                        WHERE id = %s
                    """, (rand_selected, index))
                    
                    
                conn.commit()
                print('payment_completion_timeline committed successfully.')
                print('return from dataloop 1')
            # Connect to the PostgreSQL database
        
            
            # Close the cursor and connection
            #cur.close()

        except (Exception, psycopg2.DatabaseError) as error:
            print(f"An error occurred: {error}")

        finally:
            if conn is not None:
                #conn.close()
                print('Database connection closed.')
        print('return from dataloop 2')
        time.sleep(10)

    cur.close()
    conn.close()


def dataloopMain():
    createLoanReqTable()
    firstInsertInReqTable()
    secondInsertInReqTable()
    #updateReqTable()