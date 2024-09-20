print(
    "################################################################################"
)
print("Use standard python libraries to do the transformations")
print(
    "################################################################################"
)

# Question: How do you read data from a CSV file at ./data/sample_data.csv into a list of dictionaries?
# import csv module
import csv

# Path to the CSV file
file_path = './data/sample_data.csv'

# Initialize an empty list to store dictionaries
my_store = []

# Open and read the CSV file
with open(file_path, mode='r', newline='') as csvfile:
    csv_reader = csv.DictReader(csvfile)
    
    # Iterate through each row and append to the list
    for row in csv_reader:
        my_store.append(dict(row))

#Print the first few dictionaries to check the data
for entry in my_store[:5]:
    print(entry)

# Question: How do you remove duplicate rows based on customer ID?
no_same = []
my_data = set()

for row in my_store:
    if row["customerID"] not in my_data:
        no_same.append(row)
        my_data.add(row["customerID"])
    else:
        print("This is a duplicate ID")    
# Question: How do you handle missing values by replacing them with 0?
for row in data:
    if not row["a"]:
        row["a"] = 0
# Question: How do you remove outliers such as age > 100 or purchase amount > 1000?
for i in range(len(data) - 1, -1, -1):
    if data[i]["age"] > 100 or data[i]["purchase_amount"] > 1000:
        del data[i]  

# Question: How do you convert the Gender column to a binary format (0 for Female, 1 for Male)?
for row in data:
    if row["Gendar"] == "Female":
        row["Gender"] = 0
    elif row["Gendar"] == "Male":
        row["Gendar"] = 1   
# Question: How do you split the Customer_Name column into separate First_Name and Last_Name columns?
for row in data:
    First_Name, Last_Name = row["Customer_Name"].split(" ", 1)
    row["First_Name"] = First_Name
    row["Last_Name"] = Last_Name
# Question: How do you calculate the total purchase amount by Gender?
total_purchase_by_gender = {}
for row in data:
    total_purchase_by_gender[row["Gender"]] += float(row["Purchase_Amount"])
# Question: How do you calculate the average purchase amount by Age group?
# assume age_groups is the grouping we want
# hint: Why do we convert to float?
age_groups = {"18-30": [], "31-40": [], "41-50": [], "51-60": [], "61-70": []}

for row in data:
    age = int(row["Age"])
    if age <= 30:
        age_groups["18-30"].append(float(row["Purchase_Amount"]))
    elif age <= 40:
        age_groups["31-40"].append(float(row["Purchase_Amount"]))
    elif age <= 50:
        age_groups["41-50"].append(float(row["Purchase_Amount"]))
    elif age <= 60:
        age_groups["51-60"].append(float(row["Purchase_Amount"]))
    else:
        age_groups["61-70"].append(float(row["Purchase_Amount"]))

average_purchase_by_age_group = {
    group: sum(amounts) / len(amounts) for group, amounts in age_groups.items()
}

# Question: How do you print the results for total purchase amount by Gender and average purchase amount by Age group?
your_total_purchase_amount_by_gender = {} # your results should be assigned to this variable
average_purchase_by_age_group = {} # your results should be assigned to this variable

print(f"Total purchase amount by Gender: {your_total_purchase_amount_by_gender}")
print(f"Average purchase amount by Age group: {average_purchase_by_age_group}")

print(
    "################################################################################"
)
print("Use DuckDB to do the transformations")
print(
    "################################################################################"
)

# Question: How do you connect to DuckDB and load data from a CSV file into a DuckDB table?
# Connect to DuckDB and load data

# Read data from CSV file into DuckDB table

# Question: How do you remove duplicate rows based on customer ID in DuckDB?

# Question: How do you handle missing values by replacing them with 0 in DuckDB?

# Question: How do you remove outliers (e.g., age > 100 or purchase amount > 1000) in DuckDB?

# Question: How do you convert the Gender column to a binary format (0 for Female, 1 for Male) in DuckDB?

# Question: How do you split the Customer_Name column into separate First_Name and Last_Name columns in DuckDB?

# Question: How do you calculate the total purchase amount by Gender in DuckDB?

# Question: How do you calculate the average purchase amount by Age group in DuckDB?

# Question: How do you print the results for total purchase amount by Gender and average purchase amount by Age group in DuckDB?
print("====================== Results ======================")
print("Total purchase amount by Gender:")
print("Average purchase amount by Age group:")
