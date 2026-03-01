import pandas as pd

# Sample employee data with intentional quality issues
data = {
    "id": [1, 2, 3, 4, 5],
    "name": [
        "Frank Wilson",
        "Grace Lee",
        "Henry Brown",
        "Iris Taylor",
        "Jack Anderson",
    ],
    "email": [
        "frank@company.com",
        "grace-no-at",
        "henry@company.com",
        "iris@company.com",
        "jack@company.com",
    ],
    "department": ["Engineering", "Finance", "Marketing", "Sales", "HR"],
    "salary": [105000, 88000, -10000, 92000, 79000],
    "hire_date": ["2023-02-10", "2023-04-15", "2023-06-20", "2022-12-05", "2024-02-14"],
    "status": ["active", "active", "inactive", "active", "active"],
}

df = pd.DataFrame(data)

# Create Excel file
excel_path = "d:/Github/_SaaS/lakelogic/examples/03_data_sources/files/excel/data/employees.xlsx"
df.to_excel(excel_path, index=False, sheet_name="Employees", engine="openpyxl")

print(f"Excel file created: {excel_path}")
print(f"Shape: {df.shape}")
print(df)
