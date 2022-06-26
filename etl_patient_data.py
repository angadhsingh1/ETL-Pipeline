import pypyodbc as odbc 
import pandas as pd 
import mysql.connector


def data_process(df):
    """
    Function Purpose:
    Takes the data read from the csv file and processes it. Filters out the unwanted values; 
    adds a column 'avg_glucose_mg/dl' which contains the average of glucose level in blood at different times;
    adds a column 'Diabete_type' which determines the patient's stage of diabete

    Assumption for Test Cases: 
    My data clean up is based on the fact that we will only be using columns: 'glucose_mg/dl_t1', 'glucose_mg/dl_t2', 'glucose_mg/dl_t3'. To find 
    average of glucose level in different times and diabete type; these were the only parameters that I thought of to be useful. 
    Hence, I have not accounted for null or empty values in the fields like email. 

    Possible Test Cases relating (based on our assumption): 
    -> Considering that the data may have the following values: negative values, values greater that 400 (which may add outliers to the data). 
    To normalize the data we need to clean these outlier which has been done under the comment '# Adding column of Diabete_type'. 

    -> The required data may have values like na, n/a, null or ' '. To account for these values I am reading only the data that does not have these values.
    This code can be found under __init__ function call under comment '# 1. Importing dataset from CSV'.

    """

    list_numer = ['glucose_mg/dl_t1', 'glucose_mg/dl_t2', 'glucose_mg/dl_t3']       # limiting the columns in dataframe required for our aim. 
    df = list_numerate(list_numer, df)                                              # Limits the values of columns to normalize the data.
    avg_df = avg_gluc_level(df)                                                     # Add a column that calculates the average of all three glucose measurement time points.
    df['avg_glucose_mg/dl'] = avg_df                                                # Adding column of avg_glucose_mg/dl

    df.loc[df["avg_glucose_mg/dl"] > 200.00, "Diabete_type"] = "Diabetes"           # Adding column of Diabete_type based on the average glucose level in the body
    df.loc[df["avg_glucose_mg/dl"] < 140.00, "Diabete_type"] = "Normal"
    df.loc[(df["avg_glucose_mg/dl"] >= 140.00) & (df["avg_glucose_mg/dl"] <= 200.00), "Diabete_type"] = "Pre-Diabetes"

    return df


def required_data(df):
    """
    Purpose:
    This function specify columns to inserted to the SQL database.

    Assumption: 
    I am assuming that the following columns are considered as Protected health information (PHI): 
    first_name, last_name, Email, Address

    Justificaiton for removing email:
    The fact that I only need my email to login to access my health information account (in BC).

    Data in our database:
    ['patient_id', 'glucose_mg/dl_t1', 'glucose_mg/dl_t2', 'glucose_mg/dl_t3', 'avg_glucose_mg/dl', 'Diabete_type']
    Since 'patient_id' is unique so it will act as primary_key for our database

    """
    columns = ['patient_id', 'glucose_mg/dl_t1', 'glucose_mg/dl_t2', 'glucose_mg/dl_t3', 'avg_glucose_mg/dl', 'Diabete_type']   # Columns filtered to be insterted into the databse
    df_data = df[columns]                       
    records = df_data.values.tolist()                                                                                           # storing them in a list to add them to sql databse
    return records


def list_numerate(list_numer, df):
    """
    Purpose: 
    Limits the values of 'glucose_mg/dl_t1', 'glucose_mg/dl_t2', 'glucose_mg/dl_t3' to be range of 0 to 400, to normalize the data. 

    Assumption: 
    Any value outside the given range of 0 to 400 will be an outlier. To normalize the data, it is important to filter the values outside this range.

    Input:
    list_numer: Column names for the data that I relevant to our goal of finding average glucose level and patient has a diabete or not.
    df: Contains the data read from the patient_data.csv file.  

    """
    for col in list_numer:
        filter_df = ((df[col] > 0) & (df[col] < 400))                   # limiting the range of glucose level values 
        df = df.loc[filter_df]
    return df


def avg_gluc_level(df):
    """
    Purpose: 
    Add a column that calculates the average of all three glucose measurement time points.

    Input:
    df: Contains the data read from the patient_data.csv file.  

    Output:
    Returns the average glucose level value in blood from the readings at three different times. Rounds up the average value upto 2 decimals. 
    """
    return round((df['glucose_mg/dl_t1'] + df['glucose_mg/dl_t2'] + df['glucose_mg/dl_t3'])/3, 2)


if __name__ == "__main__":
    
    """
    Purpose: 
    This is the main function where I am calling all my code from different functions created. 
    
    """

    # 1. Importing dataset from CSV
    """
    Relates back to assumption in 'data_process()' function

    Assumption:
    The data might might contain (null, na, n/a, ' ') values. To account for those missing data values
    Here missing_values is used to justify for the possible missing data in the file.
    """
    missing_values = ["n/a", "na", " "]
    df = pd.read_csv('patient_data.csv', encoding='unicode_escape',  na_values = missing_values)
    
    # 2. Processing data to filter the unwanted data
    df = data_process(df)

    # 3. Gathering the data required to go into database and storing it in a list
    records = required_data(df)

    # 4. Create SQL Servre Connection String
    mydb = mysql.connector.connect(
        host="localhost",
        user="username",
        password="123",
        database="Patient_Data"
    )

    """
    Note for above statement:
    Not entering any specific values in the 'host' and 'user' values for mydb for the puposes of your testing.
    However, I am commenting 2 lines of code for you to look at the output for final data that will be entered in the database.
    
    Uncomment the lines below for testing the data output;
    """
    # for val in records:
        # print(val)

    # 5. Creating a cursor so as to insert values in the sql database. 
    mycursor = mydb.cursor()
    sql = "INSERT INTO patient_data (patient_id, gl_t1, gl_t2, gl_t3, avg_gl, diab_type) VALUES (%f, %f, %f, %f, %f, %s)"

    # 6. Inserting the values using 'sql' and 'records' variables.
    mycursor.executemany(sql, records)

    # 7. Commit and closing the connection. 
    mydb.commit()
    mydb.close()
    print(mycursor.rowcount, "was inserted.")

# Program Ends Here. 