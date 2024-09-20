import polars as pl
from cuallee import Check, CheckLevel

# Read CSV file into Polars DataFrame
df = pl.read_csv("./data/sample_data.csv")

# Question: Check for Nulls on column Id and that Customer_ID column is unique
# check docs at https://canimus.github.io/cuallee/polars/ on how to define a check and run it.
# you will end up with a dataframe of results, check that the `status` column does not have any "FAIL" in it

# Initialize the Check object
check = Check()

# Define checks
check.is_complete("Id")  
check.is_unique("Customer_ID")  

# Run the checks
results = check.validate(df)

# Display results
print(results)

# Check that status column does not have fail
if (results.filter(pl.col("status") == "FAIL").shape[0]) == 0:
    print("All checks passed!")
else:
    print("Some checks failed.")
