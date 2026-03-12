import psycopg2
import time
import random
import pandas as pd


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


db_params = {
    "host"    : "localhost",
    "database": "ruleToData",
    "user"    : "ruleToData",
    "password": "rule2Data",
    "port"    : "5432"  # Default PostgreSQL port
}

age = []
no_of_kids = []
occupation_type_list = ['salary_earner','pensioneer','sme_owner']
lower_thirty_occupations = ['salary_earner','student','student & business owner']
marital_status_list = ['single','married','divorced']
no_of_cars = []
no_of_houses = []
lower_thirty_residential_list = ['residential estate','low-cost housing','student neighbourhood']
residential_location_list = ['residential estate','low-cost housing','village','government housing']
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




above_thirty_income = list(range(150000, 1600000, 7430))

lower_income = [v for v in above_thirty_income if v < 400000]
middle_income = [v for v in above_thirty_income if v >= 400000 and v<700000]
upper_income = [v for v in above_thirty_income if v >= 700000]

lower_spending_above = list(range(100000, 150000, 3150))
middle_spending_above = list(range(150000, 390000, 6000))
upper_spending_above = list(range(400000, 600000, 10250))





def createProfile():

    conn = None

    try:
        # Connect to the PostgreSQL database
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**db_params)
        
        # Create a cursor object
        cur = conn.cursor()
        
        # Execute a simple query to verify the connection
        cur.execute("""CREATE TABLE IF NOT EXISTS borrower_profile (id SERIAL PRIMARY KEY ,
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
        print('Table gets created')
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"An error occurred: {error}")

    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')





    conn = psycopg2.connect(**db_params) 
        
        # Create a cursor object
    cur = conn.cursor() 
    for element in names:
    

        try:
            # Connect to the PostgreSQL database
            print('Connecting to the PostgreSQL database...')
        
            name = element
            age = random.choice(list(range(18,50,4))) 
        
            no_of_kids = 0
            if age< 30:
                no_of_kids = random.choice(list(range(0,2,1)))
            else:
                no_of_kids = random.choice(list(range(0,8,1)))
            marital_status = ''
            if age< 30:
                marital_status = random.choice(['single','married'])
            else :
                marital_status = random.choice(['single','married','divorced','widow','widower'])
            no_of_cars = random.choice(list(range(0,8,1))) 
            no_of_houses = random.choice(list(range(0,8,1))) ,

            residential_location = ''
            if age<30:
                residential_location = random.choice(lower_thirty_residential_list)
            else:
                residential_location = random.choice(residential_location_list)

            gender = random.choice(gender_list)

            occupation_type = ''
            business_valuation = 0
            number_of_staffs = 0 
            mobility_index = 0


            if age<30:
                occupation_type = random.choice(lower_thirty_occupations)
                if occupation_type == 'business_owner':
                    business_valuation = random.choice(list(range(200000,2000000,13400)))
                    number_of_staffs = random.choice(list(range(0,9,1)))
                    mobility_index = random.choice(list(range(0,20,3)))
            else:
                occupation_type = random.choice(occupation_type_list)
                if occupation_type == 'business_owner':
                    business_valuation = random.choice(list(range(200000,2000000,13400)))
                    number_of_staffs = random.choice(list(range(0,9,1)))
                    mobility_index = random.choice(list(range(0,20,3)))

            average_monthly_income = 0
            average_monthly_spend = 0
            if age<30:
                randValue = random.random()
                if randValue <= 0.85 and randValue > 0.35:
                    average_monthly_income = random.choice(middle_lower_thirty)
                    average_monthly_spend = random.choice(middle_spending)
                elif randValue <= 0.35:
                    average_monthly_income = random.choice(lower_thirty)
                    average_monthly_spend = random.choice(lower_spending)

                else :
                    average_monthly_income = random.choice(upper_lower_thirty)
                    average_monthly_spend = random.choice(upper_spending)

            else:
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
            INSERT INTO borrower_profile (name,age,no_of_kids,marital_status,no_of_cars,no_of_houses,residential_location,occupation_type,average_monthly_income,
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


def insertProfile():
    conn = psycopg2.connect(**db_params)      
    # Create a cursor object
    cur = conn.cursor()
    cur.execute("SELECT * FROM borrower_profile")
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    newDf = pd.DataFrame(rows,columns=columns)


    while 'jay':
        #conn = None
        try:
            index = random.choice( list(range(len(newDf))))
            age = newDf['age'][index]
            target_amount_list = ['100000-300000','300000-500000','500000-800000','800000-1000000','1000000+']
            payment_completion_timeline_list = ['3','5','7','9','12','24']

            target_amount = 'target_amount'
            payment_completion_timeline = 'payment_completion_timeline'

            list_of_vars = ['no_of_cars','no_of_houses','residential_location','average_monthly_income']

            selectedSamples = random.sample(list_of_vars,2)
            appendedCols = []
            appendedDict = {}
            for key in selectedSamples:
                if key == 'no_of_cars':
                    no_of_cars = random.choice(list(range(0,8,1))) 
                    appendedCols.append('no_of_cars')
                    appendedDict['no_of_cars'] = no_of_cars
                elif key == 'no_of_houses':
                    no_of_houses = random.choice(list(range(0,8,1))) 
                    appendedCols.append('no_of_houses')
                    appendedDict['no_of_houses'] = no_of_houses
                elif key == 'residential_location':
                    if age<30:
                        dataList = lower_thirty_residential_list.copy()
                        dataList.remove(newDf['residential_location'][index])
                        residential_location = random.choice(dataList)
                        appendedDict['residential_location'] = residential_location
                    else:
                        dataList = residential_location_list.copy()
                        dataList.remove(newDf['residential_location'][index])
                        residential_location = random.choice(dataList)
                        appendedDict['residential_location'] = residential_location
                    appendedCols.append('residential_location')

                elif key == 'average_monthly_income':
                    average_monthly_income = 0
                    average_monthly_spend = 0

                    appendedCols.append('average_monthly_income')
                    appendedCols.append('average_monthly_spend')
                    if age<30:
                        randValue = random.random()
                        if randValue <= 0.85 and randValue > 0.35:
                            average_monthly_income = random.choice(middle_lower_thirty)
                            average_monthly_spend = random.choice(middle_spending)
                        elif randValue <= 0.35:
                            average_monthly_income = random.choice(lower_thirty)
                            average_monthly_spend = random.choice(lower_spending)

                        else :
                            average_monthly_income = random.choice(upper_lower_thirty)
                            average_monthly_spend = random.choice(upper_spending)

                    else:
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
            
                    appendedDict['average_monthly_income'] = average_monthly_income
                    appendedDict['average_monthly_spend'] = average_monthly_spend
            


            if 'average_monthly_income' in appendedCols:
                query = f"""
                    UPDATE borrower_profile
                    SET {appendedCols[0]} = %s,
                        {appendedCols[1]} = %s,
                        {appendedCols[2]} = %s
                    WHERE id = %s
                """

                print(f'objects printend are {appendedDict} and columns are {appendedCols}')
                cur.execute(query, (appendedDict[appendedCols[0]],appendedDict[appendedCols[1]],appendedDict[appendedCols[2]], index))
                conn.commit()
                

            else:
                query = f"""
                    UPDATE borrower_profile
                    SET {appendedCols[0]} = %s,
                        {appendedCols[1]} = %s
                    WHERE id = %s
                """

                print(f'objects printend are {appendedDict} and columns are {appendedCols}')
                cur.execute(query, (appendedDict[appendedCols[0]],appendedDict[appendedCols[1]], index))
                conn.commit()
                

       
                    
                    
                
            #print('payment_completion_timeline committed successfully.')
            #print('return from dataloop 1')
                # Connect to the PostgreSQL database
            
                
                # Close the cursor and connection
                #cur.close()

        except (Exception, psycopg2.DatabaseError) as error:
            print(f"An error occurred: {error}")

        finally:
            if conn is not None:
                #conn.close()
                print('Database connection closed.')
        #print('return from dataloop 2')
        time.sleep(30)

    cur.close()
    conn.close()

def ProfilePipe():
    createProfile()
    insertProfile()

ProfilePipe()