import mysql.connector

# db = mysql.connector.connect(user='root', password = 'admin', host = "127.0.0.1", db="Northwind_test")
# cursor = db.cursor()

class Functions:
    @staticmethod
    def data_reader(csv):
        csv_row = []
        with open(csv, "r", encoding="utf8") as file:
            for line in file:
                csv_row.append(line)
        return csv_row


    @staticmethod
    def sql_executer(command):
        db = mysql.connector.connect(host="127.0.0.1", user="root", password="admin", db="Northwind_test")
        cursor = db.cursor()
        try:
            cursor.execute(command)
            db.commit()
        except:
            db.rollback()
            db.close()

class Customer:
    def persist(self):
        sql = "INSERT INTO `Northwind_test`.`customers` (`CustomerID`, `CompanyName`, `ContactName`, `ContactTitle`, " \
              "`Address`, `City`, `Region`, `PostalCode`, `Country`, `Phone`, `Fax`) VALUES \
        ('" + self.customerid + "', '" + self.companyname + "', '" + self.contactname + "', '" + self.contacttitle\
              + "', '" + self.address + "', '" + self.city + "', '" + self.region + "', '" + self.postalcode + "', '" +\
              self.country + "', '" + self.phone + "', '" + self.fax + "');"
        Functions.sql_executer(sql)

    @staticmethod
    def parse(row):
        parsed_row = row.split(";")
        customer = Customer()
        customer.customerid = parsed_row[0]
        customer.companyname = parsed_row[1]
        customer.contactname = parsed_row[2]
        customer.contacttitle = parsed_row[3]
        customer.address = parsed_row[4]
        customer.city = parsed_row[5]
        customer.region = parsed_row[6]
        customer.postalcode = parsed_row[7]
        customer.country = parsed_row[8]
        customer.phone = parsed_row[9]
        customer.fax = parsed_row[10]
        return customer

    def caller_customer(self):
        rows = Functions.data_reader("customers.csv")
        for i in range(1, len(rows)):
            customers = Customer.parse(rows[i])
            customers.persist()

test_customer = Customer()
test_customer.caller_customer()

class Employees:
    def persist(self):
        sql = "INSERT INTO 'Northwind_test'.'employees' ('EmployeeID', 'LastName', 'FirstName', 'Title', 'TitleOfCourtesy','BirthDate', 'HireDate', 'Address', 'City', 'Region', 'PostalCode', 'Country', 'HomePhone', 'Extension', 'Photo', 'Notes', 'ReportsTo', 'PhotoPath', 'Salary') VALUES \
              ('" + self.EmployeeID + "', '" + self.LastName + "', '" + self.FirstName + "', '" + self.Title + "', '" + self.TitleOfCourtesy\
              + "', '" + self.BirthDate + "', '" + self.HireDate + "', '" + self.Address + "', " \
         "'" + self.City + "', '" + self.Region + "', '" + self.PostalCode +  "', '" + self.Country +  "', '" + self.HomePhone\
              + "', '" + self.Extension +  "', '" + self.Photo +  "', '" + self.Notes + "', '" \
              + self.ReportsTo +  "', '" + self.PhotoPath +  "', '" + self.Salary + "');"
        Functions.sql_executer(sql)


    @staticmethod
    def parse(row):
        parsed_row = row.split(";")
        employee = Employees()
        employee.EmployeeID = parsed_row[0]
        employee.LastName = parsed_row[1]
        employee.FirstName = parsed_row[2]
        employee.Title = parsed_row[3]
        employee.TitleOfCourtesy = parsed_row[4]
        employee.BirthDate = parsed_row[5]
        employee.HireDate = parsed_row[6]
        employee.Address = parsed_row[7]
        employee.City = parsed_row[8]
        employee.Region = parsed_row[9]
        employee.PostalCode = parsed_row[10]
        employee.Country  = parsed_row[11]
        employee.HomePhone = parsed_row[12]
        employee.Extension = parsed_row[13]
        employee.Photo = parsed_row[14]
        employee.Notes = parsed_row[15]
        employee.ReportsTo = parsed_row[16]
        employee.PhotoPath = parsed_row[17]
        employee.Salary = parsed_row[18]
        return employee

    def caller_employee(self):
        rows = Functions.data_reader("employees.csv")
        for i in range(1, len(rows)):
            employe = Employees.parse(rows[i])
            employe.persist()

test_employe = Employees()
test_employe.caller_employee()

