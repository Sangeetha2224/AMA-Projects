import pandas as pd
import mysql.connector
from django.http import HttpResponse
from django.shortcuts import render
from .forms import ExcelUploadForm
from io import BytesIO
import zipfile
import numpy as np


def upload_files(request):
    if request.method == 'POST':
        form = ExcelUploadForm(request.POST, request.FILES)
        if form.is_valid():
            file1 = request.FILES['file1']
            file2 = request.FILES['file2']
            
            print(f"File1: {file1.name}, File2: {file2.name}")

            # Check file1 extension and load accordingly
            if file1.name.endswith('.xlsx') or file1.name.endswith('.xls'):
                df1 = pd.read_excel(file1, sheet_name=None)  # Load all sheets from file1
                if 'Data Entry' in df1:
                    df1 = df1['Data Entry']  # Access the 'Data Entry' sheet
            elif file1.name.endswith('.csv'):
                df1_combined = pd.read_csv(file1)  # Load CSV file
                df1 = df1_combined  # Assign df1 to df1_combined
            else:
                return HttpResponse("Unsupported file format for file1.")

            # Load punch data (file2)
            if file2.name.endswith('.xlsx') or file2.name.endswith('.xls'):
                df2 = pd.read_excel(file2)
            elif file2.name.endswith('.csv'):
                df2 = pd.read_csv(file2)
            else:
                return HttpResponse("Unsupported file format for file2.")

            # Fill missing values for numeric and object columns in df1
            if isinstance(df1, pd.DataFrame):
                df1['wage'] = pd.to_numeric(df1['wage'], errors='coerce').fillna(0).astype(int)  # Replace non-numeric values with 0
                for col in df1.select_dtypes(include='number').columns:
                    df1[col] = df1[col].fillna(0)  # Fill with 0 for numeric columns

                for col in df1.select_dtypes(include='object').columns:
                    df1[col] = df1[col].fillna('')  # Fill with '' for string columns

            # Prepare punch data for insertion
            punch_data = df2.fillna('').values.tolist()

            # Connect to MySQL Database
            connection = mysql.connector.connect(
                host="localhost",
                user="root",
                password="Abc@123",
                database="attedance"
            )
            cursor = connection.cursor()
            cursor.execute("SELECT DATABASE();")
            print(f"Connected to database: {cursor.fetchone()}")
            
            cursor.execute("""TRUNCATE TABLE attedance.temp;""")
            cursor.execute("""TRUNCATE TABLE attedance.punch;""")

            # Create temporary tables for the uploaded data
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS temp (
                    S.No INT, 
                    Unique_Id VARCHAR(255), 
                    DATE VARCHAR(255),  
                    MONTH_NUMBER INT, 
                    MILL VARCHAR(255), 
                    EMP ID VARCHAR(255), 
                    Employee_Name VARCHAR(255), 
                    Department VARCHAR(255), 
                    Category VARCHAR(255), 
                    SHIFT VARCHAR(255), 
                    Pr/Ab/Wkoff VARCHAR(255), 
                    HOURS INT, 
                    FINE VARCHAR(255), 
                    over time hours VARCHAR(255), 
                    MEALS ALLOWANCE VARCHAR(255), 
                    TEA VARCHAR(255), 
                    EXTRA SIDER ALLOWANCE VARCHAR(255), 
                    Fullday/Halfday(1/0.5) INT, 
                    OT DAYS VARCHAR(255), 
                    wage INT, 
                    costing VARCHAR(255)
                );
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS punch (
                    Date VARCHAR(255), 
                    Pay Code VARCHAR(255),
                    Card No VARCHAR(255),
                    Employee Name VARCHAR(255),
                    Punch-1 VARCHAR(255),
                    Punch-2 VARCHAR(255), 
                    Punch-3 VARCHAR(255),
                    Punch-4 VARCHAR(255)
                );
            """)

            # Insert data into the temp table (employee_data from df1)
            try:
                employee_data = df1[['S.No', 'Unique_Id', 'DATE', 'MONTH_NUMBER', 'MILL', 'EMP ID',
                                               'Employee_Name', 'Department', 'Category', 'SHIFT',
                                               'Pr/Ab/Wkoff', 'HOURS', 'over time hours',
                                               'MEALS ALLOWANCE', 'TEA', 'EXTRA SIDER ALLOWANCE',
                                               'Fullday/Halfday(1/0.5)', 'OT DAYS', 'wage', 'costing']].values.tolist()
                print("Employee Data (before conversion):", employee_data[:5]) 
                employee_data = [tuple(row) for row in employee_data]
                print("Employee Data (after conversion to tuples):", employee_data[:5]) 

                cursor.executemany("""
                    INSERT INTO temp (S.No, Unique_Id, DATE, MONTH_NUMBER, MILL, EMP ID, 
                                      Employee_Name, Department, Category, SHIFT, 
                                      Pr/Ab/Wkoff, HOURS, over time hours, 
                                      MEALS ALLOWANCE, TEA, EXTRA SIDER ALLOWANCE, 
                                      Fullday/Halfday(1/0.5), OT DAYS, wage, costing)              
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, employee_data)
                connection.commit()
                print(f"Inserted {cursor.rowcount} rows into temp.")
            except Exception as e:
                print(f"Error inserting into temp: {e}")
                return HttpResponse(f"Error inserting data into MySQL: {str(e)}", status=500)
   
            try:
                cursor.executemany("""
                    INSERT INTO punch (Date, Pay Code, Card No, Employee Name, 
                                       Punch-1, Punch-2, Punch-3, Punch-4) 
                    VALUES (%s, %s, %s,%s, %s, %s, %s,%s)
                """, punch_data)
                connection.commit()
                print(f"Inserted {cursor.rowcount} rows into punch.")
            except Exception as e:
                print(f"Error inserting into punch: {e}")
            
           

            query = """
                SELECT DISTINCT
                    e.EMP ID, 
                    e.Unique_Id, 
                    e.DATE, 
                    e.MONTH_NUMBER, 
                    e.MILL, 
                    e.Employee_Name, 
                    e.Department, 
                    e.Category, 
                    e.SHIFT, 
                    e.Pr/Ab/Wkoff, 
                    e.HOURS, 
                    e.over time hours, 
                    e.MEALS ALLOWANCE, 
                    p.Pay Code,
                    CASE 
                        WHEN p.Punch-4 LIKE '%i' THEN p.Punch-4
                        WHEN p.Punch-3 LIKE '%i' THEN p.Punch-3
                        WHEN p.Punch-2 LIKE '%i' THEN p.Punch-2
                        WHEN p.Punch-1 LIKE '%i' THEN p.Punch-1
                        ELSE NULL
                    END AS Result
                FROM 
                    punch p 
                JOIN 
                    temp e ON p.Pay Code = e.EMP ID 
                WHERE 
                    (p.Punch-1 LIKE '%i' OR 
                     p.Punch-2 LIKE '%i' OR 
                     p.Punch-3 LIKE '%i' OR 
                     p.Punch-4 LIKE '%i');
            """
            cursor.execute(query)
            result = cursor.fetchall()

            query_2 = """
                SELECT DISTINCT
                    e.EMP ID, 
                    e.Unique_Id, 
                    e.DATE, 
                    e.MONTH_NUMBER, 
                    e.MILL, 
                    e.Employee_Name, 
                    e.Department, 
                    e.Category, 
                    e.SHIFT, 
                    e.Pr/Ab/Wkoff, 
                    e.HOURS, 
                    e.over time hours, 
                    e.MEALS ALLOWANCE, 
                    p.Pay Code,
                    CASE 
                        WHEN p.Punch-4 LIKE '%o' THEN p.Punch-4
                        WHEN p.Punch-3 LIKE '%o' THEN p.Punch-3
                        WHEN p.Punch-2 LIKE '%o' THEN p.Punch-2
                        WHEN p.Punch-1 LIKE '%o' THEN p.Punch-1
                        ELSE NULL
                    END AS Result
                FROM 
                    punch p 
                JOIN 
                    temp e ON p.Pay Code = e.EMP ID
                WHERE 
                    (p.Punch-1 LIKE '%o' OR 
                     p.Punch-2 LIKE '%o' OR 
                     p.Punch-3 LIKE '%o' OR 
                     p.Punch-4 LIKE '%o');
            """
            cursor.execute(query_2)
            result_2 = cursor.fetchall()

            query_3 = """
                SELECT DISTINCT
    e.EMP ID, 
    e.MEALS ALLOWANCE, 
    e.over time hours, 
    e.HOURS, 
    e.Pr/Ab/Wkoff, 
    e.SHIFT, 
    e.Department,  
    e.Category, 
    e.MILL, 
    e.MONTH_NUMBER, 
    e.Date, 
    e.Unique_Id, 
    e.Employee_Name
   
FROM 
    attedance.temp e
WHERE 
    NOT EXISTS (
        SELECT 1
        FROM attedance.punch p
        WHERE e.EMP ID = p.Pay Code
    )

                
            """
            cursor.execute(query_3)
            result_3 = cursor.fetchall()

            print(f"Number of records fetched for Matching I: {len(result)}")
            print(f"Number of records fetched for Matching O: {len(result_2)}")
            print(f"Number of records fetched for Not Matching: {len(result_3)}")

            columns = ['EMP ID', 'Unique_Id', 'DATE', 'MONTH_NUMBER', 'MILL', 'Employee_Name', 'Department', 
                       'Category', 'SHIFT', 'Pr/Ab/Wkoff', 'HOURS', 'over time hours', 
                       'MEALS ALLOWANCE', 'Pay Code', 'Result']
            columns3 = ['EMP ID', 'MEALS ALLOWANCE', 'Overtime_Hours', 'HOURS', 'Pr/Ab/Wkoff', 
                        'SHIFT', 'Department', 'Category', 'MILL', 'MONTH_NUMBER', 
                        'DATE', 'Unique_Id', 'Employee_Name']

            # Create DataFrames from the result of the SQL queries
            result_df = pd.DataFrame(result, columns=columns)
            result_df_2 = pd.DataFrame(result_2, columns=columns)
            result_df_3 = pd.DataFrame(result_3, columns=columns3)

            # Close the cursor and connection
            cursor.close()
            connection.close()

            # Export the results to Excel files
            output = BytesIO()
            output_2 = BytesIO()
            output_3 = BytesIO()
            # Perform SQL queries to process the data
            # (Your existing logic for processing the data goes here)

            

            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                result_df.to_excel(writer, index=False, sheet_name='Matching I')

            with pd.ExcelWriter(output_2, engine='openpyxl') as writer_2:
                result_df_2.to_excel(writer_2, index=False, sheet_name='Matching O')
            


            with pd.ExcelWriter(output_3, engine='openpyxl') as writer_3:
               result_df_3.to_excel(writer_3, index=False, sheet_name='Not Matching')

            # Create a zip archive of all the Excel outputs
            zip_buffer = BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w') as zf:
                zf.writestr('matching_I.xlsx', output.getvalue())
                zf.writestr('matching_O.xlsx', output_2.getvalue())
                zf.writestr('not_matching.xlsx', output_3.getvalue())

            zip_buffer.seek(0)

            # Send the zip file as a response
            response = HttpResponse(zip_buffer, content_type='application/zip')
            response['Content-Disposition'] = 'attachment; filename="output_files.zip"'
            return response

    else:
        form = ExcelUploadForm()

    return render(request, 'upload.html', {'form': form})