from cProfile import label
from distutils.util import execute
from operator import truediv
from re import I, L
import sqlite3

def get_connection():
    """Returns a connection object for the application database, 
        with the row factory set to Row so that row data can be referenced using
        either index or column names"""
    connection = sqlite3.connect("data.sqlite")

    # Allow for indexing of rows using either integers or column names
    # See https://docs.python.org/3/library/sqlite3.html#row-objects
    connection.row_factory = sqlite3.Row

   # Enforce referential integrity
    cursor = connection.cursor()
    cursor.execute("PRAGMA foreign_keys = ON")

    return connection

def get_user(username):
    """Gets the user with the given username as a dict containing 
        the keys 'username' and 'password_hash'"""
    # TODO: Complete this method as per the docstring above -COMPLETED
    with get_connection() as cnx:
        sql = """SELECT username, password_hash FROM user WHERE username = ?"""
        user_info = cnx.execute(sql,[username]).fetchall()
        for row in user_info:
            user = {'username':row['username'],
                    'password_hash':row['password_hash']
                    }
    return user

def update_password(username, password_hash):
    """Updates the password hash for the given username"""
    # TODO: Complete this method as per the docstring above -COMPLETED
    with get_connection() as cnx:
        sql = """UPDATE user SET password_hash = ? WHERE username = ?"""
        cnx.execute(sql,[password_hash,username])


def add_person(data):
    """Inserts a new person row based on the given data (must be a dict with keys corresponding to the
        column names of the person table).
        The person data may also include a 'phone_numbers' array that contains any number of 
        phone number dicts of the form {'number','label'}
        """
    try: 
        # Transaction begins with the 'with' block statement as per sqlite3 library
        # isolation_level parameter is set as default, therefore using plain BEGIN statement
        with get_connection() as cnx:
            cursor = cnx.cursor()
            # BEGIN statement is issued implicitly before the first INSERT (DML) query (autocommit mode is turned off)  
            sql = """INSERT INTO person 
                    (first_name, last_name, birthday, email,
                    address_line1, address_line2, city, prov, country, postcode)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
            cursor.execute(sql, [
                data['first_name'],
                data['last_name'],
                data['birthday'],
                data['email'],
                data['address_line1'],
                data['address_line2'],
                data['city'],
                data['prov'],
                data['country'],
                data['postcode']
            ])

            # TODO: Insert the person's phone numbers too, and make sure to do this in the context of a transaction 
            # COMPLETED   
            id = cursor.lastrowid
            if (data['phone_numbers']):          
                for item in data['phone_numbers']:
                    values = list(item.values())
                    values.append(id)
                    cursor.executemany("INSERT INTO phone ('number', label, person_id) VALUES (?, ?, ?)",[values])

    # If no errors raised, the changes are automaticly commited once the "with" block is executed.
    
    # The "except" block catches any transaction exception, prints a fail message and call the rollback function
    # on the connection to discard database changes.         
    except Exception as e:
        print(e)
        print("Couldn't add a person")
        cnx.rollback()
            

def delete_person(id):
    """Deletes the person with the given id from the person table
        id must be an id that exists in the person table"""
    with get_connection() as cnx:
        cursor = cnx.cursor()
        sql = """DELETE FROM person WHERE person_id = ?"""
        return cursor.execute(sql, [id])

PERSON_SORTABLE_FIELDS = ('person_id','first_name','last_name','birthday','email')
PERSON_SORTABLE_FIELD_HEADINGS = ('ID','First Name','Last Name','Birthday','Email')

def person_in_list(person, people):
    """This helper function returns true if a person has the same id in the 
        given list of people.
        people is a list of dicts with a 'person_id' key.
        person is the current person dict 
        """   
    for p in people:
        if p['person_id'] == person['person_id']:
            return True  

def get_people_list(order_by):

    assert order_by in PERSON_SORTABLE_FIELDS, "The order_by argument must be one of: " + ", ".join(PERSON_SORTABLE_FIELDS)

    with get_connection() as cnx:
        cursor = cnx.cursor()
        # TODO: Update this query to include phone number information as per lab instructions - COMPLETED
        sql = """SELECT p.person_id, p.first_name, p.last_name, p.birthday, p.email, ph."number", ph.label, 
                        p.address_line1, p.address_line2, p.city, p.prov, p.country, p.postcode
                    FROM person AS p
                        LEFT JOIN phone AS ph USING (person_id)"""
        
        if order_by:
            sql += " ORDER BY " + order_by

        results = cursor.execute(sql).fetchall()

        people = []
        # TODO: Update this loop so that it correctly adds a phone number list to each person - COMPLETED
        for r in results:
            person = {
                'person_id': r['person_id'],
                'first_name': r['first_name'],
                'last_name': r['last_name'],
                'birthday': r['birthday'],
                'email': r['email'],
                'phone_numbers': [{'number': r['number'], 'label': r['label']}], 
                'address_line1': r['address_line1'],
                'address_line2': r['address_line2'],
                'city': r['city'],
                'prov': r['prov'],
                'country': r['country'],
                'postcode': r['postcode'],
            }

            if not person_in_list(person, people):
                people.append(person)
            else:
                people[-1]['phone_numbers'].append(person['phone_numbers'][0])    
            
        return people

def get_person_ids():
    """Returns a list of the person ids that exist in the database"""
    with get_connection() as cnx:
        cursor = cnx.cursor()
        sql = """SELECT person_id FROM person"""
        return [ row[0] for row in cursor.execute(sql).fetchall() ]

# if __name__ == "__main__":
#     d = {
#                 'first_name': 'fred',
#                 'last_name': 'falconi',
#                 'birthday': '1111-33-44',
#                 'email': 'd@d.com', 
#                 'address_line1': 'address_line1',
#                 'address_line2': 'address_line2',
#                 'city': 'city',
#                 'prov': 'prov',
#                 'country': 'country',
#                 'postcode': 'postcode',
#                 'phone_numbers': [{'number': '888-888-8888', 'label': 'CELL'}, 
#                                     {'number': '666-666-6666', 'label': 'WORK'}],
#             }
#     add_person(d)
#     # l = d['phone_numbers'][0].values()
#     # print(d['phone_numbers'][0].values())
#     # print(l)
